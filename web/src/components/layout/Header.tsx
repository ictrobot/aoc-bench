import { NavLink, useNavigate, useLocation } from "react-router"
import { Moon, Sun } from "lucide-react"
import { Combobox } from "@/components/ui/combobox.tsx"
import { useTheme } from "@/hooks/use-theme.ts"
import { useUrlHostBenchmark } from "@/hooks/use-url-state.tsx"
import { withQuery } from "@/lib/routes.ts"

const navItems = [
  { to: "/", label: "Dashboard" },
  { to: "/timeline", label: "Timeline" },
  { to: "/impact", label: "Impact" },
]

export function Header() {
  const { host: currentHost, hostIndex, index, setHost, bench } = useUrlHostBenchmark()
  const hosts = Object.keys(index.hosts).sort()
  const { isDark, toggle } = useTheme()
  const navigate = useNavigate()
  const { pathname } = useLocation()
  const benchmarkOptions = hostIndex.benchmarks.map((b) => ({
    value: b.name,
    label: b.name,
  }))
  const currentBench = pathname === "/benchmark" ? bench : ""

  return (
    <header className="border-b bg-background">
      <div className="mx-auto flex max-w-7xl flex-wrap items-center gap-3 px-4 py-3">
        <NavLink
          to={withQuery("/", { host: currentHost })}
          className="order-1 mr-auto text-lg font-semibold transition-colors hover:text-muted-foreground md:mr-0"
        >
          aoc-bench
        </NavLink>

        <nav className="order-3 flex w-full flex-wrap gap-x-4 gap-y-1 md:order-2 md:w-auto">
          {navItems.map(({ to, label }) => (
            <NavLink
              key={to}
              to={withQuery(to, { host: currentHost })}
              className={({ isActive }) =>
                `text-sm transition-colors hover:text-foreground ${
                  isActive ? "text-foreground font-medium" : "text-muted-foreground"
                }`
              }
            >
              {label}
            </NavLink>
          ))}
        </nav>

        <Combobox
          ariaLabel="Search benchmarks"
          value={currentBench}
          onChange={(bench) => navigate(withQuery("/benchmark", { host: currentHost, bench }))}
          options={benchmarkOptions}
          placeholder="Search benchmarks…"
          searchPlaceholder="Search benchmarks…"
          className="order-4 w-full min-[480px]:w-[260px] md:order-3 md:w-[220px]"
        />

        <div className="order-2 flex items-center gap-3 md:order-4 md:ml-auto">
          <button
            type="button"
            onClick={toggle}
            className="inline-flex size-9 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
            aria-label={isDark ? "Switch to light mode" : "Switch to dark mode"}
            title={isDark ? "Switch to light mode" : "Switch to dark mode"}
          >
            {isDark ? <Sun size={16} /> : <Moon size={16} />}
          </button>
          {hosts.length > 1 && (
            <Combobox
              ariaLabel="Select host"
              value={currentHost}
              onChange={setHost}
              options={hosts.map((h) => ({ value: h, label: h }))}
              placeholder="Select host"
              className="w-[170px] min-[480px]:w-[220px] md:w-[190px]"
            />
          )}
        </div>
      </div>
    </header>
  )
}
