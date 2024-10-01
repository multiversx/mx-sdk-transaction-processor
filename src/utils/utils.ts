export function selectMany<TIn, TOut>(array: TIn[], predicate: (input: TIn) => TOut[]): TOut[] {
  const result = [];

  for (const item of array) {
    result.push(...predicate(item));
  }

  return result;
}
