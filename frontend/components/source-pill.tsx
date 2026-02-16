import { cn, sourceColor } from "@/lib/utils";

interface SourcePillProps {
  slug: string;
  name?: string;
  className?: string;
}

export function SourcePill({ slug, name, className }: SourcePillProps) {
  const color = sourceColor(slug);
  const displayName = name ?? slug.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium",
        className
      )}
      style={{ backgroundColor: `${color}20`, color }}
    >
      <span
        className="h-1.5 w-1.5 rounded-full"
        style={{ backgroundColor: color }}
      />
      {displayName}
    </span>
  );
}
