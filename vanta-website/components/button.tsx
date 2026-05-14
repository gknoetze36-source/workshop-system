import Link from "next/link";
import type { ReactNode } from "react";

type ButtonProps = {
  children: ReactNode;
  href: string;
  variant?: "primary" | "secondary";
};

export function Button({ children, href, variant = "primary" }: ButtonProps) {
  const styles =
    variant === "primary"
      ? "bg-chartreuse text-gunmetal shadow-glow hover:shadow-[0_0_70px_rgba(224,255,79,0.24)]"
      : "border border-white/12 bg-white/[0.04] text-porcelain hover:border-chartreuse/40 hover:bg-chartreuse/[0.06]";

  return (
    <Link
      href={href}
      className={`group inline-flex items-center justify-center rounded-full px-5 py-3 text-sm font-semibold transition duration-300 ${styles}`}
    >
      <span>{children}</span>
      <span className="ml-2 inline-block transition duration-300 group-hover:translate-x-1">-&gt;</span>
    </Link>
  );
}
