import React, { useState, useRef, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { supabase } from "@/integrations/supabase/client";
import DealCard from "@/components/DealCard";
import type { Deal } from "@/hooks/usePersonalizedDeals";
import { Send, Loader2, Sparkles, AlertTriangle } from "lucide-react";
import ReactMarkdown from "react-markdown";

interface Message {
  role: "user" | "assistant";
  content: string;
  deals?: Deal[];
}

const DAILY_LIMIT = 10;

// GPT sometimes outputs "- **Key:** value" inline on one line separated by spaces.
// This converts those into proper newline-separated markdown bullets.
function normalizeMarkdown(text: string): string {
  return text.replace(/ - (\*\*)/g, "\n- $1");
}

const GREETING = "Hallo! Ich bin der SnapFare Agent 🛫 Ich helfe dir, die besten Flugdeals ab der Schweiz zu finden. Frag mich z.B. nach günstigen Asien-Deals, Business-Flügen oder dem besten Angebot diesen Sommer!";

const SUGGESTIONS = [
  "Zeig mir günstige Asien-Deals",
  "Gibt es Business-Deals unter CHF 2000?",
  "Was kostet ZRH→NYC nächsten Monat?",
];

interface DealsChatPanelProps {
  userName?: string;
}

const DealsChatPanel: React.FC<DealsChatPanelProps> = ({ userName }) => {
  const greeting = userName ? GREETING.replace("Hallo!", `Hallo, ${userName}!`) : GREETING;
  const [messages, setMessages] = useState<Message[]>([{ role: "assistant", content: greeting }]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  // 24h message count loaded from DB on mount; incremented optimistically on each send
  const [dailyCount, setDailyCount] = useState<number | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const sessionId = useRef<string>(crypto.randomUUID());
  const persistedCount = useRef<number>(0);

  // Load 24h message count on mount (don't display old messages — fresh UI every visit)
  useEffect(() => {
    let mounted = true;
    const loadCount = async () => {
      const { data: { session } } = await supabase.auth.getSession();
      if (!mounted || !session?.user) { setDailyCount(0); return; }

      const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
      const { count } = await supabase
        .from("agent_conversations")
        .select("*", { count: "exact", head: true })
        .eq("user_id", session.user.id)
        .eq("role", "user")
        .gte("created_at", since);

      if (mounted) setDailyCount(count ?? 0);
    };
    loadCount();
    return () => { mounted = false; };
  }, []);

  const dailyUsed = dailyCount ?? 0;
  const isAtLimit = dailyUsed >= DAILY_LIMIT;
  const isNearLimit = dailyUsed >= DAILY_LIMIT - 2 && !isAtLimit;
  // Count only user messages in current session for suggestions visibility
  const sessionUserCount = messages.filter((m) => m.role === "user").length;

  const scrollToBottom = () => {
    const el = messagesContainerRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSend = async () => {
    const message = input.trim();
    if (!message || isLoading || isAtLimit || dailyCount === null) return;

    setInput("");
    const userMessage: Message = { role: "user", content: message };
    const updatedMessages = [...messages, userMessage];
    setMessages(updatedMessages);
    setDailyCount((c) => (c ?? 0) + 1); // optimistic increment
    setIsLoading(true);

    try {
      const { data: { session } } = await supabase.auth.getSession();
      if (!session?.access_token) return;

      const history = updatedMessages
        .filter((m) => m.role === "user" || m.role === "assistant")
        .slice(-8)
        .map(({ role, content }) => ({ role, content }));

      const response = await fetch(
        `${import.meta.env.VITE_SUPABASE_URL}/functions/v1/deals-chat`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${session.access_token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ message, history }),
        }
      );

      if (!response.ok) {
        const errorBody = await response.text().catch(() => "");
        console.error(`Chat API error ${response.status}:`, errorBody);
        throw new Error(`HTTP ${response.status}: ${errorBody}`);
      }

      const data = await response.json();
      const assistantText = data.response ?? "Entschuldigung, ich konnte keine Antwort generieren.";

      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: assistantText,
          deals: data.deals ?? [],
        },
      ]);

      // Persist exchange to agent_conversations (fire-and-forget)
      const msgIndex = persistedCount.current;
      persistedCount.current += 2;
      supabase.from("agent_conversations").insert([
        {
          user_id: session.user.id,
          session_id: sessionId.current,
          role: "user",
          content: message,
          message_index: msgIndex,
        },
        {
          user_id: session.user.id,
          session_id: sessionId.current,
          role: "assistant",
          content: assistantText,
          message_index: msgIndex + 1,
        },
      ]).then(({ error }) => {
        if (error) console.error("[chat] persist error:", error);
      });
    } catch (err) {
      console.error("Chat send error:", err);
      // Roll back optimistic count increment on error
      setDailyCount((c) => Math.max(0, (c ?? 1) - 1));
      const errMsg = err instanceof Error ? err.message : String(err);
      const displayMsg = errMsg.startsWith("HTTP 5")
        ? "Server-Fehler (500). Bitte versuche es nochmal."
        : errMsg.startsWith("HTTP 4")
        ? "Authentifizierungs-Fehler. Bitte neu einloggen."
        : "Es ist ein Fehler aufgetreten. Bitte versuche es nochmal.";
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: displayMsg },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="bg-white/5 border border-white/10 rounded-xl flex flex-col h-[420px]">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-white/10">
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 rounded-lg bg-gradient-to-r from-green-500 to-blue-500 flex items-center justify-center">
            <Sparkles className="w-4 h-4 text-white" />
          </div>
          <span className="text-white font-semibold text-sm">SnapFare Agent</span>
        </div>
        <span className="text-xs text-gray-600">
          {dailyCount === null ? "…" : dailyUsed}/{DAILY_LIMIT} Nachrichten heute
        </span>
      </div>

      {/* Messages */}
      <div ref={messagesContainerRef} className="flex-1 overflow-y-auto p-4 space-y-4 scrollbar-thin">
        {/* Suggestions (shown until first user message in this session) */}
        {sessionUserCount === 0 && (
          <div className="mt-2 space-y-2">
            {SUGGESTIONS.map((suggestion) => (
              <button
                key={suggestion}
                onClick={() => setInput(suggestion)}
                className="block w-full text-left text-xs bg-white/5 hover:bg-white/10 text-gray-400 hover:text-gray-200 px-3 py-2 rounded-lg border border-white/10 hover:border-white/20 transition-colors"
              >
                {suggestion}
              </button>
            ))}
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
            <div className="max-w-[85%]">
              <div
                className={`rounded-2xl px-4 py-2.5 text-sm leading-relaxed ${
                  msg.role === "user"
                    ? "bg-gradient-to-r from-green-600/80 to-blue-600/80 text-white rounded-br-sm"
                    : "bg-white/10 text-gray-200 rounded-bl-sm border border-white/10"
                }`}
              >
                {msg.role === "assistant" ? (
                  <ReactMarkdown
                    components={{
                      p: ({ children }) => <p className="my-0.5">{children}</p>,
                      ul: ({ children }) => <ul className="my-1 pl-4 list-disc space-y-0.5">{children}</ul>,
                      li: ({ children }) => <li>{children}</li>,
                      strong: ({ children }) => <strong className="font-semibold text-white">{children}</strong>,
                    }}
                  >
                    {normalizeMarkdown(msg.content)}
                  </ReactMarkdown>
                ) : (
                  msg.content
                )}
              </div>

              {/* Referenced deals */}
              {msg.deals && msg.deals.length > 0 && (
                <div className="mt-2 space-y-2">
                  {msg.deals.map((deal) => (
                    <DealCard key={deal.id} deal={deal} compact />
                  ))}
                </div>
              )}
            </div>
          </div>
        ))}

        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-white/10 border border-white/10 rounded-2xl rounded-bl-sm px-4 py-3">
              <Loader2 className="w-4 h-4 text-gray-400 animate-spin" />
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Limit warnings + Input */}
      <div className="px-3 pb-3 pt-2 border-t border-white/10">
        {isNearLimit && (
          <div className="flex items-center gap-1.5 text-xs text-amber-400 mb-2">
            <AlertTriangle className="w-3 h-3" />
            Noch {DAILY_LIMIT - dailyUsed} Nachrichten heute verfügbar
          </div>
        )}
        {isAtLimit ? (
          <div className="flex items-center gap-1.5 text-xs text-gray-500 py-2 text-center justify-center">
            <AlertTriangle className="w-3 h-3" />
            Tages-Limit erreicht. Morgen wieder {DAILY_LIMIT} Nachrichten verfügbar.
          </div>
        ) : (
          <div className="flex gap-2">
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Frag mich nach Flugdeals..."
              className="flex-1 text-sm bg-white/5 border-white/10 text-white placeholder:text-gray-600 focus:border-green-400/50 focus:ring-green-400/20 rounded-xl"
              disabled={isLoading || isAtLimit || dailyCount === null}
            />
            <Button
              onClick={handleSend}
              disabled={!input.trim() || isLoading || isAtLimit || dailyCount === null}
              size="sm"
              className="bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white rounded-xl px-3 h-9 border-0"
            >
              <Send className="w-4 h-4" />
            </Button>
          </div>
        )}
      </div>
    </div>
  );
};

export default DealsChatPanel;
