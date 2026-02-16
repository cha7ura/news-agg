"use client";

import { Nav } from "@/components/nav";
import { SourcePill } from "@/components/source-pill";
import { Badge } from "@/components/ui/badge";
import { askQuestion, searchArticles } from "@/lib/api";
import type { AskResponse, MeiliHit } from "@/lib/types";
import { cn, relativeTime } from "@/lib/utils";
import {
  ArrowRight,
  ExternalLink,
  Loader2,
  MessageSquare,
  Search,
  Sparkles,
} from "lucide-react";
import { useSearchParams } from "next/navigation";
import { Suspense, useCallback, useEffect, useRef, useState } from "react";

interface ConversationTurn {
  role: "user" | "assistant";
  content: string;
  sources?: AskResponse["sources"];
  articlesSearched?: number;
}

function SearchPageInner() {
  const searchParams = useSearchParams();
  const initialQuery = searchParams.get("q") ?? "";

  const [query, setQuery] = useState(initialQuery);
  const [mode, setMode] = useState<"search" | "ask">("ask");

  // Search results state
  const [hits, setHits] = useState<MeiliHit[]>([]);
  const [searchCount, setSearchCount] = useState(0);
  const [searchTime, setSearchTime] = useState(0);
  const [searchLoading, setSearchLoading] = useState(false);

  // Conversation state
  const [conversation, setConversation] = useState<ConversationTurn[]>([]);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [askLoading, setAskLoading] = useState(false);

  const inputRef = useRef<HTMLInputElement>(null);
  const conversationEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    conversationEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [conversation]);

  // Run initial query from URL params
  useEffect(() => {
    if (initialQuery) {
      handleSubmit(initialQuery);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleSubmit = useCallback(
    async (q?: string) => {
      const searchQuery = (q ?? query).trim();
      if (!searchQuery) return;

      if (mode === "search") {
        // Meilisearch instant results
        setSearchLoading(true);
        try {
          const result = await searchArticles({ q: searchQuery, limit: 20 });
          setHits(result.articles);
          setSearchCount(result.count);
          setSearchTime(result.processing_time_ms);
        } catch {
          setHits([]);
          setSearchCount(0);
        } finally {
          setSearchLoading(false);
        }
      } else {
        // RAG conversational answer
        setAskLoading(true);
        setConversation((prev) => [
          ...prev,
          { role: "user", content: searchQuery },
        ]);
        setQuery("");

        try {
          const result = await askQuestion({
            query: searchQuery,
            session_id: sessionId ?? undefined,
          });
          setSessionId(result.session_id);
          setConversation((prev) => [
            ...prev,
            {
              role: "assistant",
              content: result.answer,
              sources: result.sources,
              articlesSearched: result.articles_searched,
            },
          ]);
        } catch (err) {
          setConversation((prev) => [
            ...prev,
            {
              role: "assistant",
              content: `Failed to get answer: ${err instanceof Error ? err.message : "Unknown error"}`,
            },
          ]);
        } finally {
          setAskLoading(false);
        }
      }
    },
    [mode, query, sessionId]
  );

  function handleNewConversation() {
    setConversation([]);
    setSessionId(null);
    setQuery("");
    inputRef.current?.focus();
  }

  const hasConversation = conversation.length > 0;
  const hasSearchResults = hits.length > 0;

  return (
    <div className="min-h-screen">
      <Nav />
      <main className="mx-auto max-w-4xl px-4 py-8">
        {/* Header — only show when no conversation yet */}
        {!hasConversation && !hasSearchResults && (
          <div className="flex flex-col items-center gap-4 pt-16 pb-8">
            <div className="flex items-center gap-2 text-accent">
              <Sparkles size={24} />
              <h1 className="text-2xl font-bold text-text">
                Ask about Sri Lankan news
              </h1>
            </div>
            <p className="text-text-secondary text-sm max-w-md text-center">
              Get AI-powered answers with citations from 100K+ articles across
              8+ news sources
            </p>
          </div>
        )}

        {/* Mode toggle */}
        <div className="flex justify-center mb-6">
          <div className="inline-flex rounded-lg border border-border bg-surface p-1">
            <button
              onClick={() => {
                setMode("ask");
                setHits([]);
                setSearchCount(0);
              }}
              className={cn(
                "flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                mode === "ask"
                  ? "bg-accent/20 text-accent"
                  : "text-text-secondary hover:text-text"
              )}
            >
              <Sparkles size={14} />
              Ask AI
            </button>
            <button
              onClick={() => {
                setMode("search");
                setConversation([]);
                setSessionId(null);
              }}
              className={cn(
                "flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                mode === "search"
                  ? "bg-accent/20 text-accent"
                  : "text-text-secondary hover:text-text"
              )}
            >
              <Search size={14} />
              Search
            </button>
          </div>
        </div>

        {/* Conversation thread */}
        {hasConversation && (
          <div className="mb-6 space-y-4">
            {conversation.map((turn, i) => (
              <div key={i}>
                {turn.role === "user" ? (
                  <div className="flex justify-end">
                    <div className="max-w-[80%] rounded-2xl rounded-br-md bg-accent/20 px-4 py-3 text-text">
                      {turn.content}
                    </div>
                  </div>
                ) : (
                  <div className="space-y-3">
                    <div className="rounded-2xl rounded-bl-md border border-border bg-surface px-5 py-4">
                      {/* Answer text with markdown-like formatting */}
                      <div className="prose-invert text-sm text-text leading-relaxed whitespace-pre-wrap">
                        {renderCitedAnswer(turn.content, turn.sources)}
                      </div>

                      {/* Sources */}
                      {turn.sources && turn.sources.length > 0 && (
                        <div className="mt-4 border-t border-border pt-3">
                          <p className="text-xs font-medium text-text-muted mb-2">
                            Sources ({turn.articlesSearched} articles searched)
                          </p>
                          <div className="space-y-1.5">
                            {turn.sources.map((src, j) => (
                              <a
                                key={j}
                                href={src.url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="flex items-start gap-2 rounded-lg px-2 py-1.5 hover:bg-surface-raised transition-colors group"
                              >
                                <span className="flex-shrink-0 w-5 h-5 rounded-full bg-accent/20 text-accent text-xs flex items-center justify-center font-mono font-bold">
                                  {j + 1}
                                </span>
                                <div className="min-w-0 flex-1">
                                  <p className="text-xs text-text truncate group-hover:text-accent transition-colors">
                                    {src.title}
                                  </p>
                                  <p className="text-xs text-text-muted">
                                    {src.source}
                                    {src.published_at &&
                                      ` · ${src.published_at.slice(0, 10)}`}
                                  </p>
                                </div>
                                <ExternalLink
                                  size={12}
                                  className="flex-shrink-0 text-text-muted opacity-0 group-hover:opacity-100 mt-0.5"
                                />
                              </a>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            ))}

            {/* Loading indicator */}
            {askLoading && (
              <div className="flex items-center gap-2 text-text-muted text-sm px-2">
                <Loader2 size={14} className="animate-spin" />
                Searching articles and generating answer...
              </div>
            )}

            <div ref={conversationEndRef} />
          </div>
        )}

        {/* Search results (Meilisearch mode) */}
        {mode === "search" && hasSearchResults && (
          <div className="mb-6">
            <p className="text-xs text-text-muted mb-3">
              {searchCount.toLocaleString()} results in {searchTime}ms
            </p>
            <div className="space-y-3">
              {hits.map((hit) => (
                <a
                  key={hit.id}
                  href={hit.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="block rounded-lg border border-border bg-surface p-4 hover:border-accent/30 transition-colors group"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0 flex-1">
                      <h3 className="text-sm font-medium text-text group-hover:text-accent transition-colors line-clamp-2">
                        {hit.title}
                      </h3>
                      <p className="mt-1 text-xs text-text-secondary line-clamp-2">
                        {hit.excerpt}
                      </p>
                      <div className="mt-2 flex items-center gap-2 flex-wrap">
                        <SourcePill
                          slug={hit.source_slug}
                          name={hit.source_name}
                        />
                        {hit.category && (
                          <Badge variant="outline">{hit.category}</Badge>
                        )}
                        {hit.published_at && (
                          <span className="text-xs text-text-muted">
                            {relativeTime(hit.published_at)}
                          </span>
                        )}
                      </div>
                    </div>
                    <ExternalLink
                      size={14}
                      className="flex-shrink-0 text-text-muted opacity-0 group-hover:opacity-100 mt-1"
                    />
                  </div>
                </a>
              ))}
            </div>
          </div>
        )}

        {mode === "search" && searchLoading && (
          <div className="flex items-center justify-center gap-2 text-text-muted text-sm py-12">
            <Loader2 size={16} className="animate-spin" />
            Searching...
          </div>
        )}

        {/* Input area */}
        <div
          className={cn(
            "sticky bottom-0 bg-bg/80 backdrop-blur-sm pb-4 pt-2",
            !hasConversation && !hasSearchResults && "pt-0"
          )}
        >
          {hasConversation && (
            <div className="flex justify-center mb-2">
              <button
                onClick={handleNewConversation}
                className="flex items-center gap-1.5 text-xs text-text-muted hover:text-accent transition-colors"
              >
                <MessageSquare size={12} />
                New conversation
              </button>
            </div>
          )}

          <form
            onSubmit={(e) => {
              e.preventDefault();
              handleSubmit();
            }}
            className="relative"
          >
            <Search
              size={18}
              className="absolute left-4 top-1/2 -translate-y-1/2 text-text-muted"
            />
            <input
              ref={inputRef}
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={
                mode === "ask"
                  ? "Ask a question about Sri Lankan news..."
                  : "Search articles..."
              }
              className="w-full rounded-xl border border-border bg-surface text-text placeholder:text-text-muted h-12 pl-11 pr-12 text-sm focus:outline-none focus:ring-2 focus:ring-accent/50 focus:border-accent/50"
              disabled={askLoading}
            />
            <button
              type="submit"
              disabled={!query.trim() || askLoading}
              className="absolute right-2 top-1/2 -translate-y-1/2 rounded-lg bg-accent/20 p-2 text-accent hover:bg-accent/30 transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
            >
              <ArrowRight size={16} />
            </button>
          </form>

          <p className="mt-2 text-center text-xs text-text-muted">
            {mode === "ask"
              ? "Answers are generated from indexed news articles. Always verify with original sources."
              : "Full-text search powered by Meilisearch"}
          </p>
        </div>
      </main>
    </div>
  );
}

/**
 * Render answer text with citation numbers as styled badges.
 * Converts [1], [2] etc. into clickable badges that scroll to source list.
 */
function renderCitedAnswer(
  text: string,
  sources?: AskResponse["sources"]
): React.ReactNode {
  if (!sources || sources.length === 0) return text;

  const parts = text.split(/(\[\d+\])/g);
  return parts.map((part, i) => {
    const match = part.match(/^\[(\d+)\]$/);
    if (match) {
      const num = parseInt(match[1], 10);
      const source = sources[num - 1];
      if (source) {
        return (
          <a
            key={i}
            href={source.url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center justify-center w-4 h-4 rounded-full bg-accent/20 text-accent text-[10px] font-bold mx-0.5 hover:bg-accent/40 transition-colors align-super"
            title={`${source.title} (${source.source})`}
          >
            {num}
          </a>
        );
      }
    }
    return <span key={i}>{part}</span>;
  });
}

export default function SearchPage() {
  return (
    <Suspense
      fallback={
        <div className="min-h-screen">
          <Nav />
          <div className="flex items-center justify-center py-32">
            <Loader2 className="animate-spin text-text-muted" size={24} />
          </div>
        </div>
      }
    >
      <SearchPageInner />
    </Suspense>
  );
}
