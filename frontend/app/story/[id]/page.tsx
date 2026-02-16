"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { Nav } from "@/components/nav";
import { SourcePill } from "@/components/source-pill";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { StoryDetail } from "@/lib/types";
import { getStoryDetail } from "@/lib/api";
import { friendlyDate, truncate } from "@/lib/utils";
import { ArrowLeft, Clock, ExternalLink, Layers, MapPin } from "lucide-react";

export default function StoryPage() {
  const params = useParams();
  const [story, setStory] = useState<StoryDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!params.id) return;
    getStoryDetail(params.id as string)
      .then(setStory)
      .catch(() => setStory(null))
      .finally(() => setLoading(false));
  }, [params.id]);

  if (loading) {
    return (
      <div className="min-h-screen">
        <Nav />
        <div className="flex items-center justify-center py-32">
          <p className="text-text-muted font-mono text-sm animate-pulse">
            Loading story...
          </p>
        </div>
      </div>
    );
  }

  if (!story) {
    return (
      <div className="min-h-screen">
        <Nav />
        <div className="mx-auto max-w-5xl px-4 py-16 text-center">
          <p className="text-text-muted font-mono">Story not found</p>
          <Link href="/" className="text-accent text-sm mt-4 inline-block">
            Back to home
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen">
      <Nav />

      <main className="mx-auto max-w-5xl px-4 py-8">
        {/* Back link */}
        <Link
          href="/"
          className="inline-flex items-center gap-1 text-sm text-text-secondary hover:text-accent transition-colors mb-6"
        >
          <ArrowLeft size={14} />
          Back to stories
        </Link>

        {/* Story header */}
        <div className="mb-8">
          <div className="flex flex-wrap items-center gap-2 mb-3">
            {story.category && <Badge variant="accent">{story.category}</Badge>}
            {story.location && (
              <Badge variant="outline">
                <MapPin size={12} className="mr-1" />
                {story.location}
              </Badge>
            )}
            <span className="text-xs text-text-muted flex items-center gap-1">
              <Layers size={12} />
              {story.article_count} articles from {story.source_count} sources
            </span>
          </div>

          <h1 className="text-2xl md:text-3xl font-bold text-text mb-4">
            {story.title}
          </h1>

          {story.summary && (
            <p className="text-text-secondary text-base leading-relaxed max-w-3xl">
              {story.summary}
            </p>
          )}

          {/* Entities */}
          {story.entities && story.entities.length > 0 && (
            <div className="flex flex-wrap gap-1.5 mt-4">
              {story.entities.map((entity) => (
                <Badge key={entity} variant="default" className="text-xs">
                  {entity}
                </Badge>
              ))}
            </div>
          )}

          {/* Sources */}
          <div className="flex flex-wrap gap-2 mt-4">
            {story.sources?.map((s) => (
              <SourcePill key={s.slug} slug={s.slug} name={s.name} />
            ))}
          </div>
        </div>

        {/* Source comparison — article cards */}
        <h2 className="text-lg font-semibold text-text mb-4 flex items-center gap-2">
          <Layers size={18} />
          Source Coverage
        </h2>

        <div className="space-y-4">
          {story.articles?.map((article) => (
            <Card key={article.id} className="hover:border-accent/20 transition-colors">
              <CardHeader>
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2 mb-1">
                      <SourcePill
                        slug={article.source_slug}
                        name={article.source_name}
                      />
                      {article.published_at && (
                        <span className="text-xs text-text-muted flex items-center gap-1">
                          <Clock size={12} />
                          {friendlyDate(article.published_at)}
                        </span>
                      )}
                      {article.qa_score != null && (
                        <Badge
                          variant={
                            article.qa_score >= 7
                              ? "green"
                              : article.qa_score >= 4
                              ? "yellow"
                              : "red"
                          }
                          className="text-xs"
                        >
                          QA: {article.qa_score}/10
                        </Badge>
                      )}
                    </div>
                    <CardTitle className="text-base">
                      {article.title}
                    </CardTitle>
                  </div>
                  <a
                    href={article.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="shrink-0 text-text-muted hover:text-accent transition-colors"
                    title="Open original article"
                  >
                    <ExternalLink size={16} />
                  </a>
                </div>
              </CardHeader>
              <CardContent>
                {article.summary ? (
                  <p>{article.summary}</p>
                ) : article.excerpt ? (
                  <p>{truncate(article.excerpt, 300)}</p>
                ) : article.content ? (
                  <p>{truncate(article.content, 300)}</p>
                ) : (
                  <p className="text-text-muted italic">No content available</p>
                )}

                {article.author && (
                  <p className="mt-2 text-xs text-text-muted">
                    By {article.author}
                  </p>
                )}
              </CardContent>
            </Card>
          ))}
        </div>
      </main>
    </div>
  );
}
