import { useEffect, useState, type ReactNode } from "react"
import { ThemeContext } from "@/hooks/use-theme.ts"

type Theme = "light" | "dark"

/** Provide in-memory light/dark theme state for the current session. */
export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setTheme] = useState<Theme>(() =>
    window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light",
  )

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark")
  }, [theme])

  function toggle() {
    setTheme((t) => (t === "dark" ? "light" : "dark"))
  }

  return <ThemeContext value={{ isDark: theme === "dark", toggle }}>{children}</ThemeContext>
}
