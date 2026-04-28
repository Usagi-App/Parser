export function formatNumber(value: number): string {
  return new Intl.NumberFormat().format(value);
}

export function formatDate(value: string | null): string {
  if (!value) return "Not generated yet";

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "Invalid date";
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}
