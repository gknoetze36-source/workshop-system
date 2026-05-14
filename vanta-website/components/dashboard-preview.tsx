"use client";

import { motion } from "framer-motion";
import { Activity, CalendarCheck, MessageSquare, Workflow } from "lucide-react";

const rows = [
  ["Riverside Motors", "Service", "WhatsApp sent", "09:30"],
  ["Luma Salon", "Booking", "Confirmed", "10:15"],
  ["North Dental", "Reminder", "Queued", "11:00"]
];

export function DashboardPreview() {
  return (
    <motion.div
      initial={{ opacity: 0, y: 24, rotateX: 7 }}
      whileInView={{ opacity: 1, y: 0, rotateX: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.8, ease: [0.22, 1, 0.36, 1] }}
      className="glass pixel-corner relative overflow-hidden rounded-[2rem] p-4"
    >
      <div className="absolute right-12 top-10 h-24 w-24 bg-chartreuse/20 blur-3xl" />
      <div className="grid min-h-[520px] grid-cols-1 overflow-hidden rounded-[1.5rem] border border-white/10 bg-[#00191d]/80 lg:grid-cols-[210px_1fr]">
        <aside className="hidden border-r border-white/10 bg-white/[0.03] p-5 lg:block">
          <div className="mb-8 flex items-center gap-3">
            <div className="size-3 bg-chartreuse" />
            <span className="font-heading text-sm">Command OS</span>
          </div>
          {["Overview", "Bookings", "Workflows", "Messaging", "Usage"].map((item, index) => (
            <div
              key={item}
              className={`mb-2 rounded-2xl px-4 py-3 text-sm ${index === 0 ? "bg-chartreuse/10 text-chartreuse" : "text-white/50"}`}
            >
              {item}
            </div>
          ))}
        </aside>

        <main className="p-5 sm:p-7">
          <div className="mb-6 flex flex-col justify-between gap-4 sm:flex-row sm:items-center">
            <div>
              <p className="mb-2 text-xs uppercase tracking-[0.3em] text-chartreuse/80">Live tenant grid</p>
              <h3 className="font-heading text-2xl font-semibold">Automation Control</h3>
            </div>
            <div className="h-2 w-32 overflow-hidden rounded-full bg-white/10">
              <motion.div
                className="h-full bg-chartreuse"
                animate={{ width: ["35%", "86%", "52%"] }}
                transition={{ duration: 5, repeat: Infinity, ease: "easeInOut" }}
              />
            </div>
          </div>

          <div className="mb-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            {[
              ["Bookings", "1,284", CalendarCheck],
              ["Messages", "18.7k", MessageSquare],
              ["Workflows", "342", Workflow],
              ["Recovery", "31%", Activity]
            ].map(([label, value, Icon]) => (
              <div key={label as string} className="rounded-3xl border border-white/10 bg-white/[0.04] p-4">
                <Icon className="mb-5 size-5 text-chartreuse" />
                <p className="text-sm text-white/50">{label as string}</p>
                <strong className="font-heading text-2xl">{value as string}</strong>
              </div>
            ))}
          </div>

          <div className="grid gap-5 xl:grid-cols-[1.1fr_0.9fr]">
            <div className="rounded-3xl border border-white/10 bg-white/[0.035] p-5">
              <div className="mb-6 flex items-center justify-between">
                <span className="text-sm text-white/60">Workflow graph</span>
                <span className="rounded-full bg-chartreuse/10 px-3 py-1 text-xs text-chartreuse">active</span>
              </div>
              <div className="relative h-52">
                <div className="absolute left-[10%] top-[24%] h-px w-[72%] bg-gradient-to-r from-chartreuse/0 via-chartreuse/50 to-chartreuse/0" />
                {["Trigger", "Rule", "Queue", "Message"].map((node, index) => (
                  <motion.div
                    key={node}
                    animate={{ y: [0, -6, 0] }}
                    transition={{ duration: 3 + index * 0.3, repeat: Infinity, ease: "easeInOut" }}
                    className="absolute grid place-items-center rounded-2xl border border-chartreuse/20 bg-gunmetal/90 px-4 py-3 text-xs shadow-glow"
                    style={{ left: `${index * 25 + 4}%`, top: `${index % 2 ? 52 : 20}%` }}
                  >
                    <span className="mb-2 size-2 bg-chartreuse" />
                    {node}
                  </motion.div>
                ))}
              </div>
            </div>

            <div className="rounded-3xl border border-white/10 bg-white/[0.035] p-5">
              <div className="mb-4 text-sm text-white/60">Booking stream</div>
              <div className="space-y-3">
                {rows.map((row) => (
                  <div key={row.join("-")} className="grid grid-cols-[1fr_auto] gap-3 rounded-2xl border border-white/10 bg-white/[0.035] p-3 text-sm">
                    <div>
                      <div>{row[0]}</div>
                      <div className="text-white/45">{row[1]} / {row[2]}</div>
                    </div>
                    <div className="text-chartreuse">{row[3]}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </main>
      </div>
    </motion.div>
  );
}
