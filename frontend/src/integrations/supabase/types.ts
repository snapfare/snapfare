export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  // Allows to automatically instantiate createClient with right options
  // instead of createClient<Database, { PostgrestVersion: 'XX' }>(URL, KEY)
  __InternalSupabase: {
    PostgrestVersion: "12.2.3 (519615d)"
  }
  public: {
    Tables: {
      deals: {
        Row: {
          id: number
          created_at: string | null
          title: string | null
          price: number | null
          link: string | null
          currency: string | null
          image: string | null
          cabin_baggage: string | null
          aircraft: string | null
          airline: string | null
          origin: string | null
          destination: string | null
          miles: string | null
          expires_in: string | null
          booking_url: string | null
          date_in: string | null
          date_out: string | null
          cabin_class: string | null
          one_way: boolean | null
          flight: string | null
          origin_iata: string | null
          destination_iata: string | null
          date_range: string | null
          source: string | null
          scoring: string | null
          flight_duration_minutes: number | null
          flight_duration_display: string | null
          baggage_included: boolean | null
          baggage_pieces_included: number | null
          baggage_allowance_kg: number | null
          baggage_allowance_display: string | null
          llm_enriched: boolean | null
          llm_enriched_fields: Json | null
          llm_enrichment_version: string | null
        }
        Insert: {
          id?: number
          created_at?: string | null
          title?: string | null
          price?: number | null
          link?: string | null
          currency?: string | null
          image?: string | null
          cabin_baggage?: string | null
          aircraft?: string | null
          airline?: string | null
          origin?: string | null
          destination?: string | null
          miles?: string | null
          expires_in?: string | null
          booking_url?: string | null
          date_in?: string | null
          date_out?: string | null
          cabin_class?: string | null
          one_way?: boolean | null
          flight?: string | null
          origin_iata?: string | null
          destination_iata?: string | null
          date_range?: string | null
          source?: string | null
          scoring?: string | null
          flight_duration_minutes?: number | null
          flight_duration_display?: string | null
          baggage_included?: boolean | null
          baggage_pieces_included?: number | null
          baggage_allowance_kg?: number | null
          baggage_allowance_display?: string | null
          llm_enriched?: boolean | null
          llm_enriched_fields?: Json | null
          llm_enrichment_version?: string | null
        }
        Update: {
          id?: number
          created_at?: string | null
          title?: string | null
          price?: number | null
          link?: string | null
          currency?: string | null
          image?: string | null
          cabin_baggage?: string | null
          aircraft?: string | null
          airline?: string | null
          origin?: string | null
          destination?: string | null
          miles?: string | null
          expires_in?: string | null
          booking_url?: string | null
          date_in?: string | null
          date_out?: string | null
          cabin_class?: string | null
          one_way?: boolean | null
          flight?: string | null
          origin_iata?: string | null
          destination_iata?: string | null
          date_range?: string | null
          source?: string | null
          scoring?: string | null
          flight_duration_minutes?: number | null
          flight_duration_display?: string | null
          baggage_included?: boolean | null
          baggage_pieces_included?: number | null
          baggage_allowance_kg?: number | null
          baggage_allowance_display?: string | null
          llm_enriched?: boolean | null
          llm_enriched_fields?: Json | null
          llm_enrichment_version?: string | null
        }
        Relationships: []
      }
      source_articles: {
        Row: {
          id: number
          created_at: string
          article_url: string
          first_seen_at: string | null
          last_scraped_at: string | null
          status: string | null
          source: string | null
          last_error: string | null
        }
        Insert: {
          id?: number
          created_at?: string
          article_url: string
          first_seen_at?: string | null
          last_scraped_at?: string | null
          status?: string | null
          source?: string | null
          last_error?: string | null
        }
        Update: {
          id?: number
          created_at?: string
          article_url?: string
          first_seen_at?: string | null
          last_scraped_at?: string | null
          status?: string | null
          source?: string | null
          last_error?: string | null
        }
        Relationships: []
      }
      payments: {
        Row: {
          amount_cents: number | null
          created_at: string
          currency: string | null
          email: string
          id: number
          status: string | null
        }
        Insert: {
          amount_cents?: number | null
          created_at?: string
          currency?: string | null
          email: string
          id?: number
          status?: string | null
        }
        Update: {
          amount_cents?: number | null
          created_at?: string
          currency?: string | null
          email?: string
          id?: number
          status?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "payments_email_fkey"
            columns: ["email"]
            isOneToOne: false
            referencedRelation: "subscribers"
            referencedColumns: ["email"]
          },
        ]
      }
      subscribers: {
        Row: {
          created_at: string
          email: string
          source: string | null
          status: Database["public"]["Enums"]["subscriber_status"]
          tier: string
          updated_at: string
        }
        Insert: {
          created_at?: string
          email: string
          source?: string | null
          status?: Database["public"]["Enums"]["subscriber_status"]
          tier?: string
          updated_at?: string
        }
        Update: {
          created_at?: string
          email?: string
          source?: string | null
          status?: Database["public"]["Enums"]["subscriber_status"]
          tier?: string
          updated_at?: string
        }
        Relationships: []
      }
      waitlist: {
        Row: {
          created_at: string
          email: string
          id: string
          location: string | null
          updated_at: string
        }
        Insert: {
          created_at?: string
          email: string
          id?: string
          location?: string | null
          updated_at?: string
        }
        Update: {
          created_at?: string
          email?: string
          id?: string
          location?: string | null
          updated_at?: string
        }
        Relationships: []
      }
    }
    Views: {
      [_ in never]: never
    }
    Functions: {
      insert_payment_pending: {
        Args: { p_email: string }
        Returns: {
          amount_cents: number | null
          created_at: string
          currency: string | null
          email: string
          id: number
          status: string | null
        }
      }
    }
    Enums: {
      subscriber_status: "active" | "unsubscribed"
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">

type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {
      subscriber_status: ["active", "unsubscribed"],
    },
  },
} as const
