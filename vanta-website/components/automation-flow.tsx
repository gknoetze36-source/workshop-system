"use client";

import { motion } from "framer-motion";

const steps = [
  ["Trigger", "Booking, inquiry, missed visit, or payment event"],
  ["Decision", "Rules check plan, consent, timing, and industry workflow"],
  ["Queue", "Jobs are scheduled, retried, logged, and rate-limited"],
  ["Message", "WhatsApp first, SMS fallback, usage tracked automatically"]
];

export function AutomationFlow() {
  return (
    <div className="relative">
      <div className="absolute left-6 right-6 top-10 hidden h-px bg-gradient-to-r from-transparent via-chartreuse/40 to-transparent md:block" />
      <div className="grid gap-4 md:grid-cols-4">
        {steps.map(([title, body], index) => (
          <motion.div
            key={title}
            initial={{ opacity: 0, y: 22 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ delay: index * 0.12, duration: 0.55 }}
            className="relative rounded-[1.5rem] border border-white/10 bg-white/[0.035] p-5"
          >
            <motion.div
              className="mb-6 grid size-10 place-items-center bg-chartreuse text-sm font-bold text-gunmetal"
              animate={{ boxShadow: ["0 0 0 rgba(224,255,79,0)", "0 0 34px rgba(224,255,79,0.32)", "0 0 0 rgba(224,255,79,0)"] }}
              transition={{ duration: 3, repeat: Infinity, delay: index * 0.4 }}
            >
              {index + 1}
            </motion.div>
            <h3 className="mb-2 font-heading text-lg font-semibold">{title}</h3>
            <p className="text-sm leading-6 text-white/56">{body}</p>
          </motion.div>
        ))}
      </div>
    </div>
  );
}
