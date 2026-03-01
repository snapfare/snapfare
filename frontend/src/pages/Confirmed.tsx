import { CheckCircle } from "lucide-react";

const Confirmed = () => {
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center">
      <main className="px-4 sm:px-6 lg:px-8 w-full">
        <div className="max-w-xl mx-auto">
          <div className="bg-white/5 border border-white/10 rounded-2xl p-10 text-center">
            <div className="flex justify-center mb-6">
              <CheckCircle className="w-16 h-16 text-green-400" />
            </div>

            <h1 className="text-2xl sm:text-3xl font-bold text-white mb-4">
              Anmeldung bestätigt
            </h1>

            <p className="text-gray-300 text-base sm:text-lg mb-2">
              Danke! Deine E-Mail-Adresse wurde erfolgreich bestätigt.
            </p>

            <p className="text-gray-400 text-sm sm:text-base">
              Die besten Flugdeals warten in deiner Inbox ✈️
            </p>
          </div>
        </div>
      </main>
    </div>
  );
};

export default Confirmed;
