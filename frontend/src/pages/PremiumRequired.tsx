import React from "react";
import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Crown, ArrowLeft, Plane } from "lucide-react";
import { supabase } from "@/integrations/supabase/client";
import { useNavigate } from "react-router-dom";

const PremiumRequired = () => {
  const navigate = useNavigate();

  const handleSignOut = async () => {
    await supabase.auth.signOut();
    navigate("/");
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50 flex items-center justify-center px-4">
      <div className="max-w-md w-full text-center">
        {/* Icon */}
        <div className="mx-auto w-20 h-20 bg-gradient-to-r from-amber-400 to-orange-500 rounded-full flex items-center justify-center mb-6 shadow-lg">
          <Crown className="w-10 h-10 text-white" />
        </div>

        {/* Title */}
        <h1 className="text-3xl font-bold text-gray-900 mb-3">
          Premium-Zugang erforderlich
        </h1>

        <p className="text-gray-600 text-lg mb-2">
          Das personalisierte Deal-Dashboard ist exklusiv für Premium-Mitglieder.
        </p>

        <p className="text-gray-500 text-sm mb-8">
          Bereits SnapFare-Abonnent? Stelle sicher, dass du dich mit derselben
          E-Mail-Adresse wie dein Newsletter-Abo anmeldest und dass dein Konto
          aktiv ist.
        </p>

        {/* What you get */}
        <div className="bg-white/80 rounded-2xl p-6 mb-8 text-left shadow-sm border border-white">
          <h3 className="font-semibold text-gray-800 mb-4 flex items-center gap-2">
            <Plane className="w-4 h-4 text-blue-600" />
            Was du als Premium-Mitglied bekommst:
          </h3>
          <ul className="space-y-3 text-sm text-gray-600">
            <li className="flex items-start gap-2">
              <span className="text-green-500 font-bold mt-0.5">✓</span>
              <span>Personalisiertes Deal-Dashboard mit Filteroptionen</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-500 font-bold mt-0.5">✓</span>
              <span>Business Class und Meilen-Deals ab der Schweiz</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-500 font-bold mt-0.5">✓</span>
              <span>KI-Assistent für personalisierte Flugsuche</span>
            </li>
            <li className="flex items-start gap-2">
              <span className="text-green-500 font-bold mt-0.5">✓</span>
              <span>Sofortbenachrichtigungen bei neuen Top-Deals</span>
            </li>
          </ul>
        </div>

        {/* CTA Buttons */}
        <div className="space-y-3">
          <Link to="/premium">
            <Button className="w-full bg-gradient-to-r from-amber-500 to-orange-500 hover:from-amber-600 hover:to-orange-600 text-white font-semibold py-3 text-base">
              <Crown className="w-4 h-4 mr-2" />
              Premium werden
            </Button>
          </Link>

          <Link to="/">
            <Button variant="outline" className="w-full">
              <ArrowLeft className="w-4 h-4 mr-2" />
              Zurück zur Startseite
            </Button>
          </Link>
        </div>

        <button
          onClick={handleSignOut}
          className="mt-4 text-sm text-gray-400 hover:text-gray-600 underline"
        >
          Abmelden
        </button>
      </div>
    </div>
  );
};

export default PremiumRequired;
