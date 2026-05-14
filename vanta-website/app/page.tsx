import { BarChart3, BellRing, Bot, CalendarCheck, MessageSquare, Workflow } from "lucide-react";
import { AutomationFlow } from "@/components/automation-flow";
import { Button } from "@/components/button";
import { DashboardPreview } from "@/components/dashboard-preview";
import { FeatureCard } from "@/components/feature-card";
import { Footer } from "@/components/footer";
import { MotionSection } from "@/components/motion-section";
import { Navbar } from "@/components/navbar";
import { PixelParticles } from "@/components/pixel-particles";

const features = [
  {
    icon: CalendarCheck,
    title: "Automated bookings",
    description: "Convert public inquiries into structured bookings with tenant-safe routing, service logic, and follow-up recovery."
  },
  {
    icon: MessageSquare,
    title: "WhatsApp messaging",
    description: "Send confirmations, reminders, missed-booking follow-ups, and payment nudges through channel-aware messaging."
  },
  {
    icon: Bot,
    title: "AI workflows",
    description: "Use AI where it creates leverage, while fixed rules, prices, limits, and workflows keep operations predictable."
  },
  {
    icon: BellRing,
    title: "Customer reminders",
    description: "Industry-specific schedules for service reminders, checkups, appointments, recovery, and recurring work."
  },
  {
    icon: BarChart3,
    title: "Analytics",
    description: "Track bookings, usage, recovery rates, message spend, automation performance, and tenant ROI."
  },
  {
    icon: Workflow,
    title: "Business automations",
    description: "Reusable templates for workshops, salons, clinics, dentists, hotels, gyms, consultants, and repair teams."
  }
];

const logos = ["NOVA Clinics", "Apex Motors", "Luma Studio", "Orbit Dental", "Northline Hotels"];

export default function Home() {
  return (
    <main className="min-h-screen">
      <Navbar />

      <section className="relative overflow-hidden pb-24 pt-36 md:pb-32 md:pt-44">
        <PixelParticles />
        <div className="container-shell relative z-10 grid items-center gap-12 lg:grid-cols-[0.95fr_1.05fr]">
          <div>
            <div className="mb-6 inline-flex items-center gap-3 rounded-full border border-chartreuse/20 bg-chartreuse/[0.06] px-4 py-2 text-sm text-chartreuse">
              <span className="size-2 bg-chartreuse shadow-[0_0_18px_rgba(224,255,79,0.8)]" />
              Automation infrastructure for service businesses
            </div>
            <h1 className="text-balance font-heading text-5xl font-semibold tracking-[-0.06em] sm:text-6xl lg:text-7xl">
              Automate. Communicate. Grow.
            </h1>
            <p className="mt-7 max-w-2xl text-lg leading-8 text-white/62">
              VANTA Automations centralizes bookings, WhatsApp workflows, reminders, billing, analytics, and operational recovery into one premium multi-tenant SaaS platform.
            </p>
            <div className="mt-9 flex flex-col gap-3 sm:flex-row">
              <Button href="#cta">Start with VANTA</Button>
              <Button href="#dashboard" variant="secondary">View the OS</Button>
            </div>
            <div className="mt-10 grid grid-cols-3 gap-4 border-t border-white/10 pt-7">
              {[
                ["38+", "branch-ready"],
                ["99.9%", "workflow uptime"],
                ["24/7", "automation layer"]
              ].map(([value, label]) => (
                <div key={label}>
                  <div className="font-heading text-2xl font-semibold text-chartreuse">{value}</div>
                  <div className="mt-1 text-xs uppercase tracking-[0.24em] text-white/42">{label}</div>
                </div>
              ))}
            </div>
          </div>

          <DashboardPreview />
        </div>
      </section>

      <MotionSection className="container-shell py-16" delay={0.05}>
        <div className="glass rounded-[2rem] p-6">
          <div className="grid gap-8 lg:grid-cols-[0.8fr_1.2fr] lg:items-center">
            <div>
              <p className="text-sm uppercase tracking-[0.28em] text-chartreuse/80">Trusted operating layer</p>
              <h2 className="mt-3 font-heading text-3xl font-semibold tracking-tight">Built for businesses that cannot afford missed communication.</h2>
            </div>
            <div className="grid gap-5 sm:grid-cols-3">
              {[
                ["42k", "messages orchestrated"],
                ["31%", "booking recovery lift"],
                ["9", "industry templates"]
              ].map(([value, label]) => (
                <div key={label} className="rounded-3xl border border-white/10 bg-white/[0.035] p-5">
                  <strong className="font-heading text-3xl text-chartreuse">{value}</strong>
                  <p className="mt-2 text-sm text-white/52">{label}</p>
                </div>
              ))}
            </div>
          </div>
          <div className="mt-8 grid gap-3 text-sm text-white/42 sm:grid-cols-5">
            {logos.map((logo) => (
              <div key={logo} className="rounded-2xl border border-white/10 bg-white/[0.025] px-4 py-4 text-center">
                {logo}
              </div>
            ))}
          </div>
        </div>
      </MotionSection>

      <MotionSection id="platform" className="container-shell py-20">
        <div className="mb-12 max-w-3xl">
          <p className="mb-4 text-sm uppercase tracking-[0.3em] text-chartreuse/80">Platform</p>
          <h2 className="font-heading text-4xl font-semibold tracking-[-0.04em] md:text-5xl">
            One automation core. Multiple industries. Zero rebuilt systems.
          </h2>
        </div>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {features.map((feature, index) => (
            <FeatureCard key={feature.title} {...feature} index={index} />
          ))}
        </div>
      </MotionSection>

      <MotionSection id="dashboard" className="container-shell py-20">
        <div className="mb-10 grid gap-8 lg:grid-cols-[0.7fr_1fr] lg:items-end">
          <div>
            <p className="mb-4 text-sm uppercase tracking-[0.3em] text-chartreuse/80">Dashboard</p>
            <h2 className="font-heading text-4xl font-semibold tracking-[-0.04em] md:text-5xl">
              Enterprise visibility without enterprise clutter.
            </h2>
          </div>
          <p className="text-lg leading-8 text-white/58">
            Monitor bookings, tenants, automation jobs, billing usage, failed messages, and growth signals from a calm dark-mode command center.
          </p>
        </div>
        <DashboardPreview />
      </MotionSection>

      <MotionSection id="automations" className="container-shell py-20">
        <div className="mx-auto mb-12 max-w-3xl text-center">
          <p className="mb-4 text-sm uppercase tracking-[0.3em] text-chartreuse/80">Automation engine</p>
          <h2 className="font-heading text-4xl font-semibold tracking-[-0.04em] md:text-5xl">
            Event-driven workflows that feel invisible when everything works.
          </h2>
          <p className="mt-5 text-lg leading-8 text-white/58">
            VANTA turns operational events into scheduled actions, routed messages, retries, logs, and measurable recovery.
          </p>
        </div>
        <AutomationFlow />
      </MotionSection>

      <MotionSection id="pricing" className="container-shell py-20">
        <div className="grid gap-4 md:grid-cols-3">
          {[
            ["Essential", "For small teams starting with bookings and simple reminders.", "R499+"],
            ["Growth", "For multi-location businesses ready for workflow automation.", "R1,500+"],
            ["Advanced Automation", "For high-volume operators that need custom automations and priority support.", "R5,000+"]
          ].map(([name, body, price]) => (
            <div key={name} className="rounded-[2rem] border border-white/10 bg-white/[0.035] p-7">
              <h3 className="font-heading text-2xl font-semibold">{name}</h3>
              <p className="mt-4 min-h-20 leading-7 text-white/56">{body}</p>
              <div className="mt-8 font-heading text-4xl font-semibold text-chartreuse">{price}</div>
              <p className="mt-2 text-sm text-white/42">base subscription + usage scaling</p>
            </div>
          ))}
        </div>
      </MotionSection>

      <MotionSection id="cta" className="container-shell py-24">
        <div className="glass relative overflow-hidden rounded-[2.5rem] p-8 text-center md:p-14">
          <div className="absolute left-1/2 top-0 h-40 w-80 -translate-x-1/2 bg-chartreuse/15 blur-3xl" />
          <div className="relative z-10 mx-auto max-w-3xl">
            <p className="mb-4 text-sm uppercase tracking-[0.3em] text-chartreuse/80">Deploy the operating layer</p>
            <h2 className="font-heading text-4xl font-semibold tracking-[-0.04em] md:text-6xl">
              Build the business that keeps working after closing time.
            </h2>
            <p className="mx-auto mt-6 max-w-2xl text-lg leading-8 text-white/58">
              Launch one reusable automation platform for bookings, reminders, messaging, billing, and industry-specific workflows.
            </p>
            <div className="mt-9 flex justify-center">
              <Button href="mailto:hello@vantaautomations.com">Book a Strategy Call</Button>
            </div>
          </div>
        </div>
      </MotionSection>

      <Footer />
    </main>
  );
}
