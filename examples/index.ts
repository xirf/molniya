import { fromArrays, groupby } from 'molniya';

const df = fromArrays({
  dept: ['Sales', 'Sales', 'Eng', 'Eng', 'HR'],
  salary: [60000, 65000, 90000, 95000, 55000],
});

// Group by department and calculate mean salary
const result = groupby(df, ['dept'], [{ col: 'salary', func: 'mean', outName: 'avg_salary' }]);

console.log(result.toString());
