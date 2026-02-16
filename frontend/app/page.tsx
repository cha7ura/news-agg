"use client";

import { useEffect, useState } from "react";
import { Nav } from "@/components/nav";
import { SearchBar } from "@/components/search-bar";
import { StatCounter } from "@/components/stat-counter";
import { StoryCard } from "@/components/story-card";
import { Badge } from "@/components/ui/badge";
import type { Story } from "@/lib/types";
import { getTodayStories, getStories, getDashboardStats } from "@/lib/api";
import { ChevronLeft, ChevronRight, CalendarDays } from "lucide-react";

const CATEGORIES = [
  "All",
  "politics",
  "crime",
  "business",
  "sports",
  "health",
  "technology",
  "international",
];

function formatDateLabel(dateStr: string): string {
  const d = new Date(dateStr + "T12:00:00");
  const today = new Date();
  today.setHours(12, 0, 0, 0);
  const diffMs = today.getTime() - d.getTime();
  const diffDays = Math.round(diffMs / 86400000);

  if (diffDays === 0) return "Today";
  if (diffDays === 1) return "Yesterday";
  return d.toLocaleDateString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
}

function todayStr(): string {
  return new Date().toISOString().slice(0, 10);
}

function shiftDate(dateStr: string, days: number): string {
  const d = new Date(dateStr + "T12:00:00");
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}

export default function Home() {
  const [stories, setStories] = useState<Story[]>([]);
  const [totals, setTotals] = useState({ articles: 0, sources: 0, stories: 0 });
  const [activeCategory, setActiveCategory] = useState("All");
  const [loading, setLoading] = useState(true);
  const [selectedDate, setSelectedDate] = useState(todayStr());

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const isToday = selectedDate === todayStr();

        // Fetch stories for the selected date
        const storiesPromise = isToday
          ? getTodayStories()
          : getStories({ date: selectedDate, limit: 50 });

        const [storiesRes, statsRes] = await Promise.allSettled([
          storiesPromise,
          getDashboardStats(),
        ]);

        if (storiesRes.status === "fulfilled") {
          const s = storiesRes.value.stories;
          setStories(s);
          setTotals((prev) => ({ ...prev, stories: s.length }));
        }
        if (statsRes.status === "fulfilled") {
          setTotals((prev) => ({
            ...prev,
            articles: statsRes.value.totals.articles,
            sources: statsRes.value.sources.length,
          }));
        }
      } catch {
        // API unavailable — show empty state
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [selectedDate]);

  const filtered =
    activeCategory === "All"
      ? stories
      : stories.filter((s) => s.category === activeCategory);

  const featured = filtered[0];
  const rest = filtered.slice(1);

  const isToday = selectedDate === todayStr();
  const isFuture = selectedDate > todayStr();

  return (
    <div className="min-h-screen">
      <Nav />

      <main className="mx-auto max-w-7xl px-4 py-8">
        {/* Hero stats */}
        <div className="flex flex-col items-center gap-6 py-8">
          <h1 className="text-center">
            <span className="block text-3xl font-bold text-text md:text-4xl">
              Sri Lankan News
            </span>
            <span className="block text-lg text-text-secondary mt-1">
              Multi-source intelligence platform
            </span>
          </h1>

          <div className="flex gap-8 md:gap-12">
            <StatCounter label="Articles" value={totals.articles} />
            <StatCounter label="Sources" value={totals.sources} />
            <StatCounter
              label={isToday ? "Stories Today" : "Stories"}
              value={totals.stories}
            />
          </div>

          <SearchBar
            size="lg"
            placeholder="Search articles..."
            className="w-full max-w-2xl"
          />
        </div>

        {/* Date picker row */}
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <button
              onClick={() => setSelectedDate(shiftDate(selectedDate, -1))}
              className="rounded-md border border-border bg-surface p-1.5 text-text-secondary hover:text-text hover:border-accent/50 transition-colors"
              aria-label="Previous day"
            >
              <ChevronLeft size={16} />
            </button>

            <div className="relative">
              <CalendarDays
                size={14}
                className="absolute left-2.5 top-1/2 -translate-y-1/2 text-text-muted pointer-events-none"
              />
              <input
                type="date"
                value={selectedDate}
                max={todayStr()}
                onChange={(e) => {
                  if (e.target.value) setSelectedDate(e.target.value);
                }}
                className="h-8 rounded-md border border-border bg-surface pl-8 pr-2 text-sm text-text font-mono focus:outline-none focus:ring-2 focus:ring-accent/50 focus:border-accent/50 [color-scheme:dark]"
              />
            </div>

            <button
              onClick={() => setSelectedDate(shiftDate(selectedDate, 1))}
              disabled={isToday || isFuture}
              className="rounded-md border border-border bg-surface p-1.5 text-text-secondary hover:text-text hover:border-accent/50 transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
              aria-label="Next day"
            >
              <ChevronRight size={16} />
            </button>

            {!isToday && (
              <button
                onClick={() => setSelectedDate(todayStr())}
                className="ml-2 rounded-md border border-accent/30 bg-accent/10 px-2.5 py-1 text-xs font-medium text-accent hover:bg-accent/20 transition-colors"
              >
                Today
              </button>
            )}
          </div>

          <span className="text-sm font-medium text-text-secondary">
            {formatDateLabel(selectedDate)}
          </span>
        </div>

        {/* Category filters */}
        <div className="flex flex-wrap gap-2 mb-6">
          {CATEGORIES.map((cat) => (
            <button
              key={cat}
              onClick={() => setActiveCategory(cat)}
              className="transition-colors"
            >
              <Badge
                variant={activeCategory === cat ? "accent" : "outline"}
                className="cursor-pointer hover:border-accent/50"
              >
                {cat === "All" ? "All Stories" : cat}
              </Badge>
            </button>
          ))}
        </div>

        {/* Story feed */}
        {loading ? (
          <div className="flex items-center justify-center py-20">
            <div className="text-text-muted font-mono text-sm animate-pulse">
              Loading stories...
            </div>
          </div>
        ) : stories.length === 0 ? (
          <div className="rounded-lg border border-border border-dashed bg-surface/50 p-12 text-center">
            <p className="text-text-muted font-mono text-sm">
              {isToday
                ? "No stories yet. Run the clustering pipeline first:"
                : `No stories found for ${formatDateLabel(selectedDate)}`}
            </p>
            {isToday && (
              <code className="mt-2 block text-accent font-mono text-xs">
                news-agg db-migrate && news-agg cluster --hours 48
              </code>
            )}
          </div>
        ) : filtered.length === 0 ? (
          <div className="rounded-lg border border-border border-dashed bg-surface/50 p-8 text-center">
            <p className="text-text-muted font-mono text-sm">
              No stories in &quot;{activeCategory}&quot; category for{" "}
              {formatDateLabel(selectedDate)}
            </p>
          </div>
        ) : (
          <div className="space-y-6">
            {/* Featured story */}
            {featured && (
              <StoryCard story={featured} variant="featured" />
            )}

            {/* Grid of remaining stories */}
            {rest.length > 0 && (
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                {rest.map((story) => (
                  <StoryCard key={story.id} story={story} />
                ))}
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  );
}
