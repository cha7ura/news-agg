import type {
  Story,
  StoryDetail,
  SearchResult,
  MeiliSearchResult,
  AskResponse,
  DashboardStats,
} from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

async function fetchJSON<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText}`);
  }
  return res.json();
}

// --- Articles ---

export async function getArticles(params?: {
  source?: string;
  limit?: number;
  offset?: number;
}): Promise<SearchResult> {
  const sp = new URLSearchParams();
  if (params?.source) sp.set("source", params.source);
  if (params?.limit) sp.set("limit", String(params.limit));
  if (params?.offset) sp.set("offset", String(params.offset));
  const qs = sp.toString();
  return fetchJSON(`/articles${qs ? `?${qs}` : ""}`);
}

// --- Stories ---

export async function getStories(params?: {
  date?: string;
  category?: string;
  limit?: number;
  offset?: number;
}): Promise<{ stories: Story[]; total: number }> {
  const sp = new URLSearchParams();
  if (params?.date) sp.set("date", params.date);
  if (params?.category) sp.set("category", params.category);
  if (params?.limit) sp.set("limit", String(params.limit));
  if (params?.offset) sp.set("offset", String(params.offset));
  const qs = sp.toString();
  return fetchJSON(`/stories${qs ? `?${qs}` : ""}`);
}

export async function getTodayStories(): Promise<{ stories: Story[] }> {
  return fetchJSON("/stories/today");
}

export async function getStoryDetail(id: string): Promise<StoryDetail> {
  return fetchJSON(`/stories/${id}`);
}

// --- Search ---

export async function searchArticles(params: {
  q: string;
  source?: string;
  language?: string;
  category?: string;
  limit?: number;
}): Promise<MeiliSearchResult> {
  const sp = new URLSearchParams({ q: params.q });
  if (params.source) sp.set("source", params.source);
  if (params.language) sp.set("language", params.language);
  if (params.category) sp.set("category", params.category);
  if (params.limit) sp.set("limit", String(params.limit));
  return fetchJSON(`/search?${sp}`);
}

export async function askQuestion(params: {
  query: string;
  session_id?: string;
}): Promise<AskResponse> {
  return fetchJSON("/search/ask", {
    method: "POST",
    body: JSON.stringify(params),
  });
}

// --- Dashboard ---

export async function getDashboardStats(): Promise<DashboardStats> {
  return fetchJSON("/dashboard/stats");
}
