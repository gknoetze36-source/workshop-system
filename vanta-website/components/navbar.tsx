import Link from "next/link";
import { Button } from "./button";

const links = ["Platform", "Automations", "Dashboard", "Pricing"];

export function Navbar() {
  return (
    <header className="fixed left-0 right-0 top-0 z-50 border-b border-white/10 bg-gunmetal/70 backdrop-blur-2xl">
      <nav className="container-shell flex h-20 items-center justify-between">
        <Link href="/" className="flex items-center gap-3">
          <span className="grid size-9 place-items-center rounded-xl border border-chartreuse/30 bg-chartreuse/10">
            <span className="size-3 bg-chartreuse shadow-[0_0_24px_rgba(224,255,79,0.65)]" />
          </span>
          <span className="font-heading text-lg font-semibold tracking-tight">VANTA Automations</span>
        </Link>

        <div className="hidden items-center gap-8 text-sm text-white/62 md:flex">
          {links.map((link) => (
            <Link key={link} href={`#${link.toLowerCase()}`} className="transition hover:text-chartreuse">
              {link}
            </Link>
          ))}
        </div>

        <div className="hidden md:block">
          <Button href="#cta">Book a Demo</Button>
        </div>
      </nav>
    </header>
  );
}
