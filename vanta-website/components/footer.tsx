"use client";

import { motion } from "framer-motion";
import Link from "next/link";

export function Footer() {
  return (
    <footer className="relative mt-24 overflow-hidden border-t border-white/10">
      <div className="terrain absolute inset-x-0 bottom-0 h-24 opacity-25" />
      <div className="absolute bottom-20 left-0 right-0 flex h-12 items-end gap-1 opacity-50">
        {Array.from({ length: 80 }).map((_, index) => (
          <motion.span
            key={index}
            className="w-full bg-chartreuse/20"
            style={{ height: `${8 + ((index * 13) % 34)}px` }}
            animate={{ opacity: [0.16, 0.42, 0.18] }}
            transition={{ duration: 4, repeat: Infinity, delay: (index % 9) * 0.2 }}
          />
        ))}
      </div>
      <div className="container-shell relative z-10 grid gap-10 py-16 md:grid-cols-[1fr_auto]">
        <div>
          <div className="mb-4 font-heading text-2xl font-semibold">VANTA Automations</div>
          <p className="max-w-xl text-white/58">
            Intelligent automation infrastructure for service businesses that want cleaner operations, faster communication, and measurable growth.
          </p>
        </div>
        <div className="grid grid-cols-2 gap-8 text-sm text-white/56 sm:grid-cols-4">
          {["Platform", "Automations", "Pricing", "Contact"].map((item) => (
            <Link key={item} href="#" className="transition hover:text-chartreuse">
              {item}
            </Link>
          ))}
        </div>
      </div>
    </footer>
  );
}
