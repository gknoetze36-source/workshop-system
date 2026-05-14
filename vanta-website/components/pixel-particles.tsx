"use client";

import { motion } from "framer-motion";

const particles = Array.from({ length: 28 }, (_, index) => ({
  id: index,
  left: `${8 + ((index * 31) % 84)}%`,
  top: `${10 + ((index * 17) % 74)}%`,
  delay: (index % 7) * 0.35,
  size: index % 5 === 0 ? 6 : 4
}));

export function PixelParticles() {
  return (
    <div className="pointer-events-none absolute inset-0 overflow-hidden">
      {particles.map((particle) => (
        <motion.span
          key={particle.id}
          className="absolute bg-chartreuse/50"
          style={{
            left: particle.left,
            top: particle.top,
            width: particle.size,
            height: particle.size
          }}
          animate={{
            opacity: [0.08, 0.55, 0.12],
            y: [0, -18, 0],
            scale: [1, 1.4, 1]
          }}
          transition={{
            duration: 4.8,
            delay: particle.delay,
            repeat: Infinity,
            ease: "easeInOut"
          }}
        />
      ))}
    </div>
  );
}
