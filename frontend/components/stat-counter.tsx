"use client";

import { useEffect, useRef, useState } from "react";
import { cn, formatNumber } from "@/lib/utils";

interface StatCounterProps {
  label: string;
  value: number;
  className?: string;
}

export function StatCounter({ label, value, className }: StatCounterProps) {
  const [display, setDisplay] = useState(0);
  const rafRef = useRef<number>();

  useEffect(() => {
    if (value === 0) {
      setDisplay(0);
      return;
    }

    const duration = 1200;
    const start = performance.now();
    const from = 0;

    function tick(now: number) {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);
      // ease-out cubic
      const eased = 1 - Math.pow(1 - progress, 3);
      setDisplay(Math.round(from + (value - from) * eased));

      if (progress < 1) {
        rafRef.current = requestAnimationFrame(tick);
      }
    }

    rafRef.current = requestAnimationFrame(tick);
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
    };
  }, [value]);

  return (
    <div className={cn("flex flex-col items-center gap-1", className)}>
      <span className="font-mono text-2xl font-bold text-accent tabular-nums">
        {formatNumber(display)}
      </span>
      <span className="text-xs uppercase tracking-wider text-text-muted">
        {label}
      </span>
    </div>
  );
}
