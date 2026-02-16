import { Nav } from "@/components/nav";

export default function StatsPage() {
  return (
    <div className="min-h-screen">
      <Nav />
      <main className="mx-auto max-w-7xl px-4 py-8">
        <h1 className="text-2xl font-bold text-text mb-6">Pipeline Stats</h1>
        <div className="rounded-lg border border-border border-dashed bg-surface/50 p-12 text-center">
          <p className="text-text-muted font-mono text-sm">
            Stats dashboard — will show source breakdowns, ingestion activity,
            and pipeline health
          </p>
        </div>
      </main>
    </div>
  );
}
