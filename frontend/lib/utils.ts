import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";
import { formatDistanceToNow, format, isToday, isYesterday } from "date-fns";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function relativeTime(date: string | Date): string {
  const d = typeof date === "string" ? new Date(date) : date;
  return formatDistanceToNow(d, { addSuffix: true });
}

export function friendlyDate(date: string | Date): string {
  const d = typeof date === "string" ? new Date(date) : date;
  if (isToday(d)) return `Today at ${format(d, "h:mm a")}`;
  if (isYesterday(d)) return `Yesterday at ${format(d, "h:mm a")}`;
  return format(d, "MMM d, yyyy");
}

export function truncate(str: string, length: number): string {
  if (str.length <= length) return str;
  return str.slice(0, length).trimEnd() + "...";
}

export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

/** Color for each source slug — consistent across the app */
export const SOURCE_COLORS: Record<string, string> = {
  "ada-derana-en": "#ff6b35",
  "ada-derana-si": "#ff9f1c",
  "daily-mirror-en": "#2ec4b6",
  "the-island-en": "#e71d36",
  "economynext-en": "#011627",
  "colombo-gazette-en": "#7209b7",
  "news-19-si": "#3a86ff",
  "news-19-en": "#8338ec",
};

export function sourceColor(slug: string): string {
  return SOURCE_COLORS[slug] ?? "#8b949e";
}
