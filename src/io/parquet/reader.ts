/** biome-ignore-all lint/suspicious/noExplicitAny: Internal reader logic */
/** biome-ignore-all lint/style/noNonNullAssertion: Protocol guarantees */
import { Chunk } from "../../buffer/chunk.ts";
import { ColumnBuffer } from "../../buffer/column-buffer.ts";
import { createDictionary, type Dictionary } from "../../buffer/dictionary.ts";
import { DataFrame } from "../../dataframe/core.ts";
import { DType } from "../../types/dtypes.ts";
import { createSchema, type Schema } from "../../types/schema.ts";
import { snappyUncompress } from "./compression/snappy.ts";
import { convertColumn, dereferDictionary } from "./convert.ts";
import {
	bitWidth,
	type DataReader,
	readRleBitPackedHybrid,
	readVarInt,
} from "./encoding/rle.ts";
import { ThriftMetadataReader } from "./metadata_reader.ts";
import { readPlain } from "./plain.ts";
import type { ColumnChunk, FileMetaData, SchemaElement } from "./types.ts";
import { CompressionCodec, ConvertedType, Type } from "./types.ts";

interface BunFile {
	size: number;
	slice(start: number, end: number): Blob;
}

interface DataPageHeader {
	num_values?: number;
	encoding?: number;
	definition_level_encoding?: number;
	repetition_level_encoding?: number;
}

interface DictionaryPageHeader {
	num_values?: number;
	encoding?: number;
}

interface PageHeader {
	type?: number;
	uncompressed_page_size?: number;
	compressed_page_size?: number;
	data_page_header?: DataPageHeader;
	dictionary_page_header?: DictionaryPageHeader;
}

// Page type constants
const PAGE_TYPE = {
	DATA_PAGE: 0,
	INDEX_PAGE: 1,
	DICTIONARY_PAGE: 2,
	DATA_PAGE_V2: 3,
};

// Encoding constants
const ENCODING = {
	PLAIN: 0,
	PLAIN_DICTIONARY: 2,
	RLE: 3,
	BIT_PACKED: 4,
	DELTA_BINARY_PACKED: 5,
	DELTA_LENGTH_BYTE_ARRAY: 6,
	DELTA_BYTE_ARRAY: 7,
	RLE_DICTIONARY: 8,
};

// Type name mapping for readPlain
const TYPE_NAMES: Record<number, string> = {
	[Type.BOOLEAN]: "BOOLEAN",
	[Type.INT32]: "INT32",
	[Type.INT64]: "INT64",
	[Type.INT96]: "INT96",
	[Type.FLOAT]: "FLOAT",
	[Type.DOUBLE]: "DOUBLE",
	[Type.BYTE_ARRAY]: "BYTE_ARRAY",
	[Type.FIXED_LEN_BYTE_ARRAY]: "FIXED_LEN_BYTE_ARRAY",
};

export class ParquetReader {
	private file: BunFile;
	private meta: FileMetaData | null = null;

	constructor(path: string) {
		this.file = Bun.file(path);
	}

	async readMetadata(): Promise<FileMetaData> {
		if (this.meta) return this.meta;

		const fileSize = this.file.size;
		const footerLenBuf = await this.file
			.slice(fileSize - 8, fileSize)
			.arrayBuffer();
		const view = new DataView(footerLenBuf);
		const footerLen = view.getUint32(0, true);

		const magic = new TextDecoder().decode(footerLenBuf.slice(4));
		if (magic !== "PAR1") throw new Error("Invalid Parquet Magic File");

		const footerStart = fileSize - 8 - footerLen;
		const footerBuf = await this.file
			.slice(footerStart, fileSize - 8)
			.arrayBuffer();

		const thriftReader = new ThriftMetadataReader(new Uint8Array(footerBuf));
		this.meta = thriftReader.readFileMetaData();
		return this.meta;
	}

	async read(): Promise<DataFrame> {
		const meta = await this.readMetadata();
		const schema = this.convertSchema(meta);
		const dictionary = createDictionary();

		// Collect all chunks for reusable DataFrame
		const chunks: Chunk[] = [];
		for await (const chunk of this.createStream(
			meta,
			schema.value!,
			dictionary,
		)) {
			chunks.push(chunk);
		}

		return DataFrame.fromChunks(chunks, schema.value!, dictionary);
	}

	/**
	 * Stream chunks - yields one chunk per row group for memory efficiency
	 */
	async *stream(): AsyncGenerator<Chunk> {
		const meta = await this.readMetadata();
		const schema = this.convertSchema(meta);
		const dictionary = createDictionary();

		yield* this.createStream(meta, schema.value!, dictionary);
	}

	getSchema(): Schema {
		if (!this.meta) throw new Error("Must call readMetadata() first");
		return this.convertSchema(this.meta).value!;
	}

	getDictionary(): Dictionary {
		return createDictionary(); // New dictionary per call for streaming
	}

	/**
	 * Internal stream generator
	 */
	private async *createStream(
		meta: FileMetaData,
		schema: Schema,
		dictionary: Dictionary,
	): AsyncGenerator<Chunk> {
		for (const rg of meta.row_groups) {
			const chunkCols: ColumnBuffer[] = [];
			const numRows = Number(rg.num_rows);

			for (let i = 0; i < rg.columns.length; i++) {
				const colChunk = rg.columns[i];
				if (!colChunk) continue;

				const schemaElement = meta.schema[i + 1]; // +1 to skip root
				if (!schemaElement) throw new Error("Schema mismatch");

				const colData = await this.readColumn(
					colChunk,
					numRows,
					schemaElement,
					dictionary,
				);
				chunkCols.push(colData);
			}

			yield new Chunk(schema, chunkCols, dictionary);
		}
	}

	[Symbol.asyncIterator](): AsyncIterator<Chunk> {
		return this.stream();
	}

	private convertSchema(meta: FileMetaData): { value?: Schema } {
		const cols: { name: string; dtype: DType }[] = [];
		for (let i = 1; i < meta.schema.length; i++) {
			const elem = meta.schema[i];
			let dtype: DType = DType.string;
			const convertedType = elem?.converted_type;

			if (elem?.type !== undefined) {
				switch (elem.type) {
					case Type.INT96:
						dtype = DType.timestamp as DType;
						break;
					case Type.INT32:
						dtype = DType.int32 as DType;
						break;
					case Type.INT64:
						dtype = DType.int64 as DType;
						break;
					case Type.FLOAT:
						dtype = DType.float32 as DType;
						break;
					case Type.DOUBLE:
						dtype = DType.float64 as DType;
						break;
					case Type.BOOLEAN:
						dtype = DType.boolean as DType;
						break;
					case Type.BYTE_ARRAY:
					case Type.FIXED_LEN_BYTE_ARRAY:
						dtype = DType.string as DType;
						break;
				}
			}

			if (convertedType !== undefined) {
				switch (convertedType) {
					case ConvertedType.UTF8:
						dtype = DType.string as DType;
						break;
					case ConvertedType.DATE:
						dtype = DType.date as DType;
						break;
					case ConvertedType.TIMESTAMP_MILLIS:
					case ConvertedType.TIMESTAMP_MICROS:
						dtype = DType.timestamp as DType;
						break;
					case ConvertedType.TIME_MILLIS:
					case ConvertedType.TIME_MICROS:
						dtype = DType.timestamp as DType;
						break;
					case ConvertedType.UINT_8:
						dtype = DType.uint8 as DType;
						break;
					case ConvertedType.UINT_16:
						dtype = DType.uint16 as DType;
						break;
					case ConvertedType.UINT_32:
						dtype = DType.uint32 as DType;
						break;
					case ConvertedType.UINT_64:
						dtype = DType.uint64 as DType;
						break;
					case ConvertedType.INT_8:
						dtype = DType.int8 as DType;
						break;
					case ConvertedType.INT_16:
						dtype = DType.int16 as DType;
						break;
					case ConvertedType.INT_32:
						dtype = DType.int32 as DType;
						break;
					case ConvertedType.INT_64:
						dtype = DType.int64 as DType;
						break;
				}
			}

			cols.push({ name: elem?.name || "unknown", dtype });
		}

		const schemaSpec: Record<string, DType> = {};
		for (const c of cols) {
			schemaSpec[c.name] = c.dtype;
		}
		return createSchema(schemaSpec);
	}

	private async readColumn(
		chunk: ColumnChunk,
		numRows: number,
		schemaElement: SchemaElement,
		sharedDictionary: Dictionary,
	): Promise<ColumnBuffer> {
		if (!chunk.meta_data) throw new Error("Missing Column MetaData");

		// Calculate start offset (dictionary page comes first if present)
		let start = Number(chunk.meta_data.data_page_offset);
		if (
			chunk.meta_data.dictionary_page_offset !== undefined &&
			chunk.meta_data.dictionary_page_offset > 0n &&
			chunk.meta_data.dictionary_page_offset < chunk.meta_data.data_page_offset
		) {
			start = Number(chunk.meta_data.dictionary_page_offset);
		}

		const len = Number(chunk.meta_data.total_compressed_size);
		const buffer = await this.file.slice(start, start + len).arrayBuffer();
		const data = new Uint8Array(buffer);

		// Determine DType for output
		const type = chunk.meta_data.type;
		let dtype: DType = DType.string;
		const isStringColumn =
			(type === Type.BYTE_ARRAY || type === Type.FIXED_LEN_BYTE_ARRAY) &&
			schemaElement.converted_type !== ConvertedType.DECIMAL;

		switch (type) {
			case Type.INT32:
				dtype = DType.int32 as DType;
				break;
			case Type.INT64:
				dtype = DType.int64 as DType;
				break;
			case Type.FLOAT:
				dtype = DType.float32 as DType;
				break;
			case Type.DOUBLE:
				dtype = DType.float64 as DType;
				break;
			case Type.FIXED_LEN_BYTE_ARRAY:
				if (schemaElement.converted_type === ConvertedType.DECIMAL) {
					dtype = DType.float64 as DType; // Convert decimals to float64
				}
				break;
		}

		const colBuf = new ColumnBuffer(
			dtype.kind || DType.string.kind,
			numRows,
			true,
		);

		let offset = 0;
		let valuesRead = 0;
		let dictionary: unknown[] | undefined;

		// Local to Global dictionary mapping (if dictionary page exists)
		let dictionaryMapping: Uint32Array | null = null;

		while (offset < data.length && valuesRead < numRows) {
			const { header, headerLen } = this.readPageHeader(data, offset);
			offset += headerLen;

			const compressedSize =
				header.compressed_page_size ?? header.uncompressed_page_size ?? 0;
			if (compressedSize === 0) {
				throw new Error("Invalid page size");
			}
			const uncompressedSize =
				header.uncompressed_page_size ?? header.compressed_page_size ?? 0;

			let pageData = data.subarray(offset, offset + compressedSize);
			offset += compressedSize;

			// Decompress if needed
			if (chunk.meta_data.codec === CompressionCodec.SNAPPY) {
				const decomp = new Uint8Array(uncompressedSize);
				snappyUncompress(pageData, decomp);
				pageData = decomp;
			}

			// Create DataReader for page
			const reader: DataReader = {
				view: new DataView(
					pageData.buffer,
					pageData.byteOffset,
					pageData.byteLength,
				),
				offset: 0,
			};

			if (header.type === PAGE_TYPE.DICTIONARY_PAGE) {
				const numDictValues = header.dictionary_page_header!.num_values;
				if (numDictValues === undefined) {
					throw new Error("Dictionary page missing num_values");
				}
				const typeName = TYPE_NAMES[type] || "BYTE_ARRAY";
				let dictValues = readPlain(
					reader,
					typeName,
					numDictValues,
					schemaElement.type_length,
				) as any[];

				// Convert dictionary values - keep bytes for string columns for direct interning
				dictValues = convertColumn(dictValues, schemaElement, {});

				// Create mapping: Local Index -> Global SD Index
				if (isStringColumn) {
					dictionaryMapping = new Uint32Array(numDictValues);
					for (let i = 0; i < numDictValues; i++) {
						const val = dictValues[i];
						if (val instanceof Uint8Array) {
							dictionaryMapping[i] = sharedDictionary.intern(val);
						} else {
							dictionaryMapping[i] = sharedDictionary.internString(String(val));
						}
					}
				}
				// Store raw values for non-string dictionary columns (uncommon but supported)
				dictionary = dictValues;
			} else if (header.type === PAGE_TYPE.DATA_PAGE) {
				const numValues = header.data_page_header!.num_values;
				if (numValues === undefined) {
					throw new Error("Data page missing num_values");
				}
				const encoding = header.data_page_header!.encoding;

				let numActualValues = numValues;
				let definitionLevels: number[] | null = null;

				// OPTIONAL fields
				if (schemaElement.repetition_type === 1) {
					const maxDefLevel = 1;
					const defBitWidth = bitWidth(maxDefLevel);
					definitionLevels = new Array(numValues);
					readRleBitPackedHybrid(reader, defBitWidth, definitionLevels);

					let nonNulls = 0;
					for (let i = 0; i < numValues; i++) {
						if (definitionLevels[i] === maxDefLevel) nonNulls++;
					}
					numActualValues = nonNulls;
				}

				// Decode values
				let decodedValues: unknown; // Can be TypedArray or Array

				// DICTIONARY ENCODING
				if (
					encoding === ENCODING.RLE_DICTIONARY ||
					encoding === ENCODING.PLAIN_DICTIONARY
				) {
					const indexBitWidth = reader.view.getUint8(reader.offset++);

					if (isStringColumn && dictionaryMapping) {
						// OPTIMIZED PATH: Remap indices directly to global dictionary indices
						const indices = new Uint32Array(numActualValues);
						if (indexBitWidth > 0) {
							const remainingBytes = pageData.byteLength - reader.offset;
							readRleBitPackedHybrid(
								reader,
								indexBitWidth,
								indices,
								remainingBytes,
							);
						}

						// Remap in place or new array? In place is fine if we write to colBuf directly.
						// But we need to handle nulls.
						// If we have definition levels, indices only correspond to non-nulls.
						// We can write directly to colBuf.

						if (!definitionLevels) {
							// Bulk write!
							// Remap indices to global IDs
							for (let i = 0; i < numActualValues; i++) {
								indices[i] = dictionaryMapping![indices[i]!]!;
							}
							colBuf.setFromTypedArray(indices, numActualValues);
						} else {
							let valIdx = 0;
							for (let i = 0; i < numValues; i++) {
								if (definitionLevels[i] === 1) {
									colBuf.append(
										dictionaryMapping![indices[valIdx++]!]! as never,
									);
								} else {
									colBuf.appendNull();
								}
							}
						}
						decodedValues = null; // Handled
					} else {
						// Standard Dictionary Handling (Non-string or no mapping built)
						const indices: number[] = new Array(numActualValues); // Use Array for generic handling
						if (indexBitWidth > 0) {
							const remainingBytes = pageData.byteLength - reader.offset;
							readRleBitPackedHybrid(
								reader,
								indexBitWidth,
								indices,
								remainingBytes,
							);
						} else {
							indices.fill(0);
						}
						decodedValues = dereferDictionary(indices, dictionary);
					}
				} else if (encoding === ENCODING.RLE && type === Type.BOOLEAN) {
					// BOOLEAN RLE
					const boolValues = new Array(numActualValues);
					readRleBitPackedHybrid(reader, 1, boolValues);
					decodedValues = boolValues.map((v: number) => !!v);
				} else {
					// PLAIN encoding
					const typeName = TYPE_NAMES[type] || "BYTE_ARRAY";
					const rawValues = readPlain(
						reader,
						typeName,
						numActualValues,
						schemaElement.type_length,
					) as any[];
					decodedValues = convertColumn(rawValues, schemaElement, {
						keepBytes: isStringColumn,
					});
				}

				// Process decodedValues if not already handled (null)
				if (decodedValues) {
					if (!definitionLevels) {
						if (isStringColumn) {
							// String PLAIN: loop and intern
							for (let i = 0; i < (decodedValues as any).length; i++) {
								const val = (decodedValues as any)[i];
								let idx: number;
								if (val instanceof Uint8Array) {
									idx = sharedDictionary.intern(val);
								} else {
									idx = sharedDictionary.internString(String(val));
								}
								colBuf.append(idx as never);
							}
						} else if (
							(decodedValues as any).buffer &&
							(decodedValues instanceof Int32Array ||
								decodedValues instanceof Float64Array ||
								decodedValues instanceof Float32Array ||
								decodedValues instanceof BigInt64Array)
						) {
							// Numeric TypedArray: Bulk Copy
							colBuf.setFromTypedArray(decodedValues as any, numActualValues);
						} else {
							// Generic Array (Boolean, etc)
							for (let i = 0; i < (decodedValues as any).length; i++) {
								colBuf.append((decodedValues as any)[i] as never);
							}
						}
					} else {
						// With nulls
						let valIdx = 0;
						for (let i = 0; i < numValues; i++) {
							if (definitionLevels[i] === 1) {
								const val = (decodedValues as any)[valIdx++];
								if (isStringColumn) {
									let idx: number;
									if (val instanceof Uint8Array) {
										idx = sharedDictionary.intern(val);
									} else {
										idx = sharedDictionary.internString(String(val));
									}
									colBuf.append(idx as never);
								} else {
									colBuf.append(val as never);
								}
							} else {
								colBuf.appendNull();
							}
						}
					}
				}

				valuesRead += numValues;
			}
		}

		return colBuf;
	}

	private readPageHeader(
		data: Uint8Array,
		offset: number,
	): { header: PageHeader; headerLen: number } {
		const view = new DataView(
			data.buffer,
			data.byteOffset + offset,
			data.byteLength - offset,
		);
		const reader: DataReader = { view, offset: 0 };

		// Read compact thrift struct
		const header: PageHeader = {
			data_page_header: {},
			dictionary_page_header: {},
		};
		let lastFieldId = 0;

		while (true) {
			const byte = view.getUint8(reader.offset++);
			if (byte === 0) break; // STOP

			const delta = (byte & 0xf0) >> 4;
			const ftype = byte & 0x0f;

			const fieldId =
				delta === 0 ? readZigZagVarInt(reader) : lastFieldId + delta;
			lastFieldId = fieldId;

			switch (fieldId) {
				case 1:
					header.type = readZigZagVarInt(reader);
					break;
				case 2:
					header.uncompressed_page_size = readZigZagVarInt(reader);
					break;
				case 3:
					header.compressed_page_size = readZigZagVarInt(reader);
					break;
				case 4:
					readZigZagVarInt(reader);
					break; // CRC, skip
				case 5: // DataPageHeader
					this.readDataPageHeaderStruct(reader, header.data_page_header!);
					break;
				case 7: // DictionaryPageHeader
					this.readDictionaryPageHeaderStruct(
						reader,
						header.dictionary_page_header!,
					);
					break;
				default:
					skipThriftField(reader, ftype);
			}
		}

		return { header, headerLen: reader.offset };
	}

	private readDataPageHeaderStruct(
		reader: DataReader,
		out: DataPageHeader,
	): void {
		let lastFieldId = 0;
		while (true) {
			const byte = reader.view.getUint8(reader.offset++);
			if (byte === 0) break;

			const delta = (byte & 0xf0) >> 4;
			const ftype = byte & 0x0f;
			const fieldId =
				delta === 0 ? readZigZagVarInt(reader) : lastFieldId + delta;
			lastFieldId = fieldId;

			switch (fieldId) {
				case 1:
					out.num_values = readZigZagVarInt(reader);
					break;
				case 2:
					out.encoding = readZigZagVarInt(reader);
					break;
				case 3:
					out.definition_level_encoding = readZigZagVarInt(reader);
					break;
				case 4:
					out.repetition_level_encoding = readZigZagVarInt(reader);
					break;
				default:
					skipThriftField(reader, ftype);
			}
		}
	}

	private readDictionaryPageHeaderStruct(
		reader: DataReader,
		out: DictionaryPageHeader,
	): void {
		let lastFieldId = 0;
		while (true) {
			const byte = reader.view.getUint8(reader.offset++);
			if (byte === 0) break;

			const delta = (byte & 0xf0) >> 4;
			const ftype = byte & 0x0f;
			const fieldId =
				delta === 0 ? readZigZagVarInt(reader) : lastFieldId + delta;
			lastFieldId = fieldId;

			switch (fieldId) {
				case 1:
					out.num_values = readZigZagVarInt(reader);
					break;
				case 2:
					out.encoding = readZigZagVarInt(reader);
					break;
				default:
					skipThriftField(reader, ftype);
			}
		}
	}
}

function readZigZagVarInt(reader: DataReader): number {
	const n = readVarInt(reader);
	return (n >>> 1) ^ -(n & 1);
}

function skipThriftField(reader: DataReader, ftype: number): void {
	switch (ftype) {
		case 1:
		case 2:
			break; // boolean (encoded in type)
		case 3:
			reader.offset++;
			break; // byte
		case 4:
		case 5:
			readVarInt(reader);
			break; // i16/i32
		case 6:
			readVarBigInt(reader);
			break; // i64
		case 7:
			reader.offset += 8;
			break; // double
		case 8: {
			// binary
			const len = readVarInt(reader);
			reader.offset += len;
			break;
		}
		case 9:
		case 10: {
			// list/set
			const byte = reader.view.getUint8(reader.offset++);
			let size = byte >> 4;
			if (size === 15) size = readVarInt(reader);
			const elemType = byte & 0x0f;
			for (let i = 0; i < size; i++) skipThriftField(reader, elemType);
			break;
		}
		case 11: {
			// map
			const size = readVarInt(reader);
			if (size > 0) {
				const types = reader.view.getUint8(reader.offset++);
				for (let i = 0; i < size; i++) {
					skipThriftField(reader, types >> 4);
					skipThriftField(reader, types & 0x0f);
				}
			}
			break;
		}
		case 12: {
			// struct
			while (true) {
				const b = reader.view.getUint8(reader.offset++);
				if (b === 0) break;
				const d = (b & 0xf0) >> 4;
				if (d === 0) readVarInt(reader);
				skipThriftField(reader, b & 0x0f);
			}
			break;
		}
	}
}

function readVarBigInt(reader: DataReader): bigint {
	let result = 0n;
	let shift = 0n;
	while (true) {
		const byte = reader.view.getUint8(reader.offset++);
		result |= BigInt(byte & 0x7f) << shift;
		if (!(byte & 0x80)) return result;
		shift += 7n;
	}
}

export async function readParquet(path: string): Promise<DataFrame> {
	const reader = new ParquetReader(path);
	return reader.read();
}
