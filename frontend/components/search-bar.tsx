"use client";

import { Search } from "lucide-react";
import { cn } from "@/lib/utils";
import { useRouter } from "next/navigation";
import { useState, type FormEvent } from "react";

interface SearchBarProps {
  placeholder?: string;
  className?: string;
  size?: "sm" | "lg";
  onSearch?: (query: string) => void;
}

export function SearchBar({
  placeholder = "Search news...",
  className,
  size = "sm",
  onSearch,
}: SearchBarProps) {
  const router = useRouter();
  const [query, setQuery] = useState("");

  function handleSubmit(e: FormEvent) {
    e.preventDefault();
    const q = query.trim();
    if (!q) return;
    if (onSearch) {
      onSearch(q);
    } else {
      router.push(`/search?q=${encodeURIComponent(q)}`);
    }
  }

  return (
    <form onSubmit={handleSubmit} className={cn("relative", className)}>
      <Search
        size={size === "lg" ? 20 : 16}
        className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted"
      />
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder={placeholder}
        className={cn(
          "w-full rounded-lg border border-border bg-surface text-text placeholder:text-text-muted",
          "focus:outline-none focus:ring-2 focus:ring-accent/50 focus:border-accent/50",
          size === "lg"
            ? "h-14 pl-12 pr-4 text-lg"
            : "h-10 pl-9 pr-3 text-sm"
        )}
      />
    </form>
  );
}
