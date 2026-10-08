import { expect } from "bun:test";
import { Database } from "bun:sqlite";
import {
  fromRows,
  col,
  lit,
  gt,
  lt,
  eq,
  and,
  or,
  add,
  mul,
  sum,
  avg,
  min,
  max,
  count,
  type Row,
} from "../src/index.ts";
import { tc } from "./registry.ts";
import { collect, seeded } from "./helpers.ts";
import requirements from "../docs/requirements.json";

for (const seed of [1, 7, 19, 73, 101, 257, 1009, 65537])
  tc(
    {
      id: `TC-ORACLE-SQLITE-${seed}`,
      requirements: [
        "REQ-ORACLE-001",
        "REQ-NULL-001",
        "REQ-AGG-001",
        "REQ-JOIN-001",
        "REQ-PLAN-003",
      ],
      category: "differential",
      objective: `Seed ${seed} agrees with independent SQLite queries`,
      preconditions: "Fresh in-memory SQLite database; deterministic LCG seed; no shared state",
      steps: `Generate40 rows with nulls and skewed string keys and9 join rows; compare filter/projection, grouped aggregates, left join and distinct against SQL`,
      expected: "Exact ordered row equality for each query",
      cleanup: "Close SQLite in finally; engine allocations must return to zero",
    },
    async () => {
      const random = seeded(seed);
      const keys = [null, "a", "b", "c"];
      const rows = Array.from({ length: 40 }, (_, id) => ({
        id,
        k: keys[Math.floor(random() * keys.length)]!,
        x: random() < 0.3 ? null : Math.floor(random() * 12) - 3,
      }));
      const right = Array.from({ length: 9 }, (_, uid) => ({
        uid,
        k: keys[Math.floor(random() * keys.length)]!,
      }));
      const db = new Database(":memory:");
      try {
        db.exec("CREATE TABLE t(id INTEGER, k TEXT, x REAL); CREATE TABLE u(uid INTEGER, k TEXT)");
        const insert = db.query("INSERT INTO t VALUES(?,?,?)");
        for (const row of rows) insert.run(row.id, row.k, row.x);
        const insertRight = db.query("INSERT INTO u VALUES(?,?)");
        for (const row of right) insertRight.run(row.uid, row.k);
        const q = fromRows(
          { id: "i32", k: { type: "utf8", nullable: true }, x: { type: "f64", nullable: true } },
          rows,
        );
        const filtered = q
          .where(and(gt(col("x"), lit(1)), or(eq(col("k"), lit("a")), lt(col("x"), lit(5)))))
          .select({ id: col("id"), k: col("k"), v: add(mul(col("x"), lit(2)), lit(2)) })
          .sort([{ column: "id" }]);
        expect(await collect(filtered)).toEqual(
          db
            .query("SELECT id,k,x*2+2 AS v FROM t WHERE x>1 AND (k='a' OR x<5) ORDER BY id")
            .all() as Row[],
        );
        const grouped = q
          .aggregate(["k"], {
            n: count(col("x")),
            all: count(),
            s: sum(col("x")),
            a: avg(col("x")),
            lo: min(col("x")),
            hi: max(col("x")),
          })
          .sort([{ column: "k" }]);
        expect(await collect(grouped)).toEqual(
          db
            .query(
              'SELECT k,COUNT(x) n,COUNT(*) "all",SUM(x) s,AVG(x) a,MIN(x) lo,MAX(x) hi FROM t GROUP BY k ORDER BY k NULLS LAST',
            )
            .all() as Row[],
        );
        const joined = q
          .join(fromRows({ uid: "i32", k: { type: "utf8", nullable: true } }, right), {
            on: [["k", "k"]],
            how: "left",
          })
          .select({ id: col("id"), uid: col("uid") })
          .sort([{ column: "id" }, { column: "uid" }]);
        expect(await collect(joined)).toEqual(
          db
            .query("SELECT t.id,u.uid FROM t LEFT JOIN u ON t.k=u.k ORDER BY t.id,u.uid NULLS LAST")
            .all() as Row[],
        );
        expect(
          await collect(
            q
              .select({ k: col("k") })
              .distinct()
              .sort([{ column: "k" }]),
          ),
        ).toEqual(db.query("SELECT DISTINCT k FROM t ORDER BY k NULLS LAST").all() as Row[]);
      } finally {
        db.close();
      }
    },
  );

tc(
  {
    id: "TC-REGRESSION-FUSION-ORDER",
    requirements: ["REQ-PLAN-003", "REQ-EXPR-001"],
    category: "regression",
    objective: "Filter fusion preserves short-circuit suppression of unreachable overflow",
    steps: "Filter singleton by false, then by (MAX_VALUE*2)>0; optimizer fuses predicates",
    expected: "Empty result without overflow exception",
  },
  async () => {
    const q = fromRows({ x: "f64" }, [{ x: Number.MAX_VALUE }])
      .where(lit(false))
      .where(gt(mul(col("x"), lit(2)), lit(0)));
    expect(await collect(q)).toEqual([]);
  },
);

tc(
  {
    id: "TC-EVIDENCE-CATALOG",
    requirements: ["REQ-EVIDENCE-001"],
    category: "process",
    objective: "Requirement catalog has unique stable IDs and measurable statements",
    steps: "Inspect the committed catalog and validate ID uniqueness, shall wording and categories",
    expected: "All IDs unique and well formed; no empty requirements or categories",
  },
  () => {
    expect(new Set(requirements.map((requirement) => requirement.id)).size).toBe(
      requirements.length,
    );
    for (const requirement of requirements) {
      expect(requirement.id).toMatch(/^REQ-[A-Z]+-\d{3}$/);
      expect(requirement.shall).toContain("shall");
      expect(requirement.categories.length).toBeGreaterThan(0);
    }
  },
);
