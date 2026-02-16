import Link from "next/link";
import type { Story } from "@/lib/types";
import { cn, relativeTime, truncate } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { SourcePill } from "@/components/source-pill";
import { Clock, Layers } from "lucide-react";

interface StoryCardProps {
  story: Story;
  variant?: "featured" | "default" | "compact";
  className?: string;
}

export function StoryCard({
  story,
  variant = "default",
  className,
}: StoryCardProps) {
  if (variant === "compact") {
    return (
      <Link
        href={`/story/${story.id}`}
        className={cn(
          "flex items-start gap-3 rounded-lg border border-border bg-surface p-3",
          "hover:bg-surface-raised transition-colors group",
          className
        )}
      >
        <div className="min-w-0 flex-1">
          <h3 className="text-sm font-medium text-text group-hover:text-accent transition-colors line-clamp-2">
            {story.title}
          </h3>
          <div className="mt-1.5 flex items-center gap-2 text-xs text-text-muted">
            <span className="flex items-center gap-1">
              <Layers size={12} />
              {story.source_count} sources
            </span>
            <span className="flex items-center gap-1">
              <Clock size={12} />
              {relativeTime(story.first_published_at)}
            </span>
          </div>
        </div>
      </Link>
    );
  }

  const isFeatured = variant === "featured";

  return (
    <Link
      href={`/story/${story.id}`}
      className={cn(
        "group flex flex-col rounded-lg border border-border bg-surface overflow-hidden",
        "hover:border-accent/30 transition-all",
        isFeatured && "md:flex-row",
        className
      )}
    >
      {story.image_url && (
        <div
          className={cn(
            "relative bg-surface-raised overflow-hidden",
            isFeatured
              ? "md:w-2/5 aspect-video md:aspect-auto"
              : "aspect-video"
          )}
        >
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            src={story.image_url}
            alt=""
            className="h-full w-full object-cover group-hover:scale-105 transition-transform duration-300"
          />
        </div>
      )}

      <div className={cn("flex flex-1 flex-col p-4", isFeatured && "p-6")}>
        <div className="flex flex-wrap items-center gap-2 mb-2">
          {story.category && (
            <Badge variant="accent">{story.category}</Badge>
          )}
          {story.location && (
            <Badge variant="outline">{story.location}</Badge>
          )}
        </div>

        <h2
          className={cn(
            "font-semibold text-text group-hover:text-accent transition-colors",
            isFeatured ? "text-xl md:text-2xl" : "text-base",
            !isFeatured && "line-clamp-2"
          )}
        >
          {story.title}
        </h2>

        {story.summary && (
          <p
            className={cn(
              "mt-2 text-text-secondary",
              isFeatured ? "text-sm line-clamp-3" : "text-xs line-clamp-2"
            )}
          >
            {truncate(story.summary, isFeatured ? 200 : 120)}
          </p>
        )}

        <div className="mt-auto pt-3 flex items-center justify-between">
          <div className="flex flex-wrap gap-1.5">
            {story.sources?.slice(0, 4).map((s) => (
              <SourcePill key={s.slug} slug={s.slug} name={s.name} />
            ))}
            {story.sources && story.sources.length > 4 && (
              <span className="text-xs text-text-muted self-center">
                +{story.sources.length - 4}
              </span>
            )}
          </div>

          <span className="text-xs text-text-muted flex items-center gap-1 shrink-0 ml-2">
            <Clock size={12} />
            {relativeTime(story.first_published_at)}
          </span>
        </div>
      </div>
    </Link>
  );
}
