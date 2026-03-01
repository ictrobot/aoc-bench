import { render, screen } from "@testing-library/react"
import { describe, expect, it } from "vitest"
import { MarkdownLinks } from "./markdown-links.tsx"

describe("MarkdownLinks", () => {
  it("renders plain text without links", () => {
    render(<MarkdownLinks text="No links here." />)
    expect(screen.getByText("No links here.")).toBeInTheDocument()
    expect(screen.queryByRole("link")).toBeNull()
  })

  it("renders a single link", () => {
    render(<MarkdownLinks text="[Example](https://example.com)" />)
    const link = screen.getByRole("link", { name: "Example" })
    expect(link).toHaveAttribute("href", "https://example.com")
    expect(link).toHaveAttribute("target", "_blank")
    expect(link).toHaveAttribute("rel", "noopener noreferrer")
  })

  it("renders mixed text and multiple links", () => {
    const { container } = render(
      <MarkdownLinks text="A [first](https://a.example.com) and [second](https://b.example.com) link." />,
    )
    expect(container.textContent).toBe("A first and second link.")

    const links = screen.getAllByRole("link")
    expect(links).toHaveLength(2)
    expect(links[0]).toHaveTextContent("first")
    expect(links[0]).toHaveAttribute("href", "https://a.example.com")
    expect(links[1]).toHaveTextContent("second")
    expect(links[1]).toHaveAttribute("href", "https://b.example.com")
  })

  it("ignores non-https links", () => {
    const { container } = render(<MarkdownLinks text="[xss](javascript:alert(1)) and [ok](https://safe.com)" />)
    expect(screen.getAllByRole("link")).toHaveLength(1)
    expect(screen.getByRole("link", { name: "ok" })).toHaveAttribute("href", "https://safe.com")
    expect(container.textContent).toBe("[xss](javascript:alert(1)) and ok")
  })

  it("renders empty string without error", () => {
    const { container } = render(<MarkdownLinks text="" />)
    expect(container.textContent).toBe("")
  })
})
