import { cn } from "@/lib/utils";
import { type HTMLAttributes } from "react";

type BadgeVariant = "default" | "accent" | "green" | "yellow" | "red" | "outline";

const variantStyles: Record<BadgeVariant, string> = {
  default: "bg-surface-raised text-text-secondary",
  accent: "bg-accent/15 text-accent",
  green: "bg-green/15 text-green",
  yellow: "bg-yellow/15 text-yellow",
  red: "bg-red/15 text-red",
  outline: "border border-border text-text-secondary bg-transparent",
};

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  variant?: BadgeVariant;
}

export function Badge({ className, variant = "default", ...props }: BadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium transition-colors",
        variantStyles[variant],
        className
      )}
      {...props}
    />
  );
}
