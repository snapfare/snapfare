import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function getTimeGreeting(): string {
  const h = new Date().getHours();
  if (h >= 5 && h < 11) return "Guten Morgen";
  if (h >= 11 && h < 14) return "Guten Mittag";
  if (h >= 17 && h < 23) return "Guten Abend";
  return "Hallo";
}

export function getPasswordStrength(pw: string) {
  return {
    length: pw.length >= 8,
    number: /\d/.test(pw),
    special: /[^A-Za-z0-9]/.test(pw),
  };
}
