export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  public: {
    Tables: {
      deals: {
        Row: {
          id: number
          created_at: string | null
          title: string | null
          price: number | null
          currency: string | null
          link: string | null
          booking_url: string | null
          skyscanner_url: string | null
          source: string | null
          origin: string | null
          destination: string | null
          origin_iata: string | null
          destination_iata: string | null
          airline: string | null
          aircraft: string | null
          cabin_class: string | null
          stops: number | null
          date_out: string | null
          date_in: string | null
          miles: string | null
          flight_duration_minutes: number | null
          flight_duration_display: string | null
          baggage_included: boolean | null
          baggage_pieces_included: number | null
          baggage_allowance_kg: number | null
          image: string | null
          tier: string | null
          travel_period_display: string | null
          scoring: string | null
          expires_in: string | null
        }
        Insert: {
          id?: number
          created_at?: string | null
          title?: string | null
          price?: number | null
          currency?: string | null
          link?: string | null
          booking_url?: string | null
          skyscanner_url?: string | null
          source?: string | null
          origin?: string | null
          destination?: string | null
          origin_iata?: string | null
          destination_iata?: string | null
          airline?: string | null
          aircraft?: string | null
          cabin_class?: string | null
          stops?: number | null
          date_out?: string | null
          date_in?: string | null
          miles?: string | null
          flight_duration_minutes?: number | null
          flight_duration_display?: string | null
          baggage_included?: boolean | null
          baggage_pieces_included?: number | null
          baggage_allowance_kg?: number | null
          image?: string | null
          tier?: string | null
          travel_period_display?: string | null
          scoring?: string | null
          expires_in?: string | null
        }
        Update: {
          id?: number
          created_at?: string | null
          title?: string | null
          price?: number | null
          currency?: string | null
          link?: string | null
          booking_url?: string | null
          skyscanner_url?: string | null
          source?: string | null
          origin?: string | null
          destination?: string | null
          origin_iata?: string | null
          destination_iata?: string | null
          airline?: string | null
          aircraft?: string | null
          cabin_class?: string | null
          stops?: number | null
          date_out?: string | null
          date_in?: string | null
          miles?: string | null
          flight_duration_minutes?: number | null
          flight_duration_display?: string | null
          baggage_included?: boolean | null
          baggage_pieces_included?: number | null
          baggage_allowance_kg?: number | null
          image?: string | null
          tier?: string | null
          travel_period_display?: string | null
          scoring?: string | null
          expires_in?: string | null
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
          currency: string
          email: string
          id: string
          status: string
        }
        Insert: {
          amount_cents?: number | null
          created_at?: string
          currency?: string
          email: string
          id?: string
          status?: string
        }
        Update: {
          amount_cents?: number | null
          created_at?: string
          currency?: string
          email?: string
          id?: string
          status?: string
        }
        Relationships: []
      }
      subscribers: {
        Row: {
          created_at: string
          email: string
          id: string
          source: string | null
          status: Database["public"]["Enums"]["subscriber_status"]
          tier: string
          updated_at: string
        }
        Insert: {
          created_at?: string
          email: string
          id?: string
          source?: string | null
          status?: Database["public"]["Enums"]["subscriber_status"]
          tier?: string
          updated_at?: string
        }
        Update: {
          created_at?: string
          email?: string
          id?: string
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
      user_preferences: {
        Row: {
          id: string
          user_id: string
          email: string
          full_name: string | null
          onboarding_completed: boolean | null
          preferred_origins: string[]
          preferred_regions: string[]
          max_price_chf: number | null
          cabin_classes: string[]
          min_trip_days: number
          max_trip_days: number | null
          preferred_seasons: string[]
          flight_types: string[]
          include_miles_deals: boolean
          include_budget_deals: boolean
          created_at: string
          updated_at: string
        }
        Insert: {
          id?: string
          user_id: string
          email: string
          full_name?: string | null
          onboarding_completed?: boolean | null
          preferred_origins?: string[]
          preferred_regions?: string[]
          max_price_chf?: number | null
          cabin_classes?: string[]
          min_trip_days?: number
          max_trip_days?: number | null
          preferred_seasons?: string[]
          flight_types?: string[]
          include_miles_deals?: boolean
          include_budget_deals?: boolean
          created_at?: string
          updated_at?: string
        }
        Update: {
          id?: string
          user_id?: string
          email?: string
          full_name?: string | null
          onboarding_completed?: boolean | null
          preferred_origins?: string[]
          preferred_regions?: string[]
          max_price_chf?: number | null
          cabin_classes?: string[]
          min_trip_days?: number
          max_trip_days?: number | null
          preferred_seasons?: string[]
          flight_types?: string[]
          include_miles_deals?: boolean
          include_budget_deals?: boolean
          created_at?: string
          updated_at?: string
        }
        Relationships: [
          {
            foreignKeyName: "user_preferences_user_id_fkey"
            columns: ["user_id"]
            isOneToOne: true
            referencedRelation: "users"
            referencedColumns: ["id"]
          },
        ]
      }
      agent_conversations: {
        Row: {
          id: string
          user_id: string
          session_id: string
          role: string
          content: string
          message_index: number
          created_at: string
        }
        Insert: {
          id?: string
          user_id: string
          session_id: string
          role: string
          content: string
          message_index: number
          created_at?: string
        }
        Update: {
          id?: string
          user_id?: string
          session_id?: string
          role?: string
          content?: string
          message_index?: number
          created_at?: string
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
          currency: string
          email: string
          id: string
          status: string
        }
      }
    }
    Enums: {
      subscriber_status: "pending" | "active" | "unsubscribed"
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DefaultSchema = Database["public"]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof Database },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof (Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        Database[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof Database
}
  ? (Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      Database[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
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
    | { schema: keyof Database },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof Database
}
  ? Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
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
    | { schema: keyof Database },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof Database
}
  ? Database[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
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
    | { schema: keyof Database },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof Database
  }
    ? keyof Database[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof Database
}
  ? Database[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
  ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
  : never

export const Constants = {
  public: {
    Enums: {
      subscriber_status: ["pending", "active", "unsubscribed"],
    },
  },
} as const
