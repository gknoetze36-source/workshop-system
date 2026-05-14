"use client";

import { motion } from "framer-motion";
import type { LucideIcon } from "lucide-react";

export function FeatureCard({
  icon: Icon,
  title,
  description,
  index
}: {
  icon: LucideIcon;
  title: string;
  description: string;
  index: number;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 22 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ delay: index * 0.08, duration: 0.55 }}
      whileHover={{ y: -6, borderColor: "rgba(224,255,79,0.34)" }}
      className="group relative overflow-hidden rounded-[1.75rem] border border-white/10 bg-white/[0.035] p-6 transition"
    >
      <div className="absolute right-4 top-4 grid grid-cols-3 gap-1 opacity-0 transition group-hover:opacity-100">
        {Array.from({ length: 9 }).map((_, item) => (
          <span key={item} className="size-1 bg-chartreuse/50" />
        ))}
      </div>
      <div className="mb-10 grid size-12 place-items-center rounded-2xl border border-chartreuse/20 bg-chartreuse/10 text-chartreuse">
        <Icon className="size-5" />
      </div>
      <h3 className="mb-3 font-heading text-xl font-semibold">{title}</h3>
      <p className="leading-7 text-white/58">{description}</p>
    </motion.div>
  );
}
