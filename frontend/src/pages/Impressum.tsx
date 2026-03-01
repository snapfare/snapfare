const Impressum = () => {
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
      <main className="py-16 sm:py-20 lg:py-24">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="max-w-4xl mx-auto">
            <div className="bg-white/5 border border-white/10 rounded-2xl p-8 md:p-12">
              <h1 className="text-4xl font-bold mb-4 bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
                Impressum
              </h1>
              <p className="text-gray-300 text-sm mb-8">
                Dieses Impressum erfüllt die Anforderungen der Anbieterkennzeichnung im elektronischen Geschäftsverkehr in der Schweiz (UWG).*
              </p>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">Verantwortlich</h2>
                <div className="text-gray-300 leading-relaxed">
                  <p className="font-semibold">Basics by Dominic Bachmann (Einzelfirma)</p>
                  <p>Dominic Bachmann</p>
                  <p>Hasenbüelstrasse 3</p>
                  <p>8123 Ebmatingen</p>
                  <p>Schweiz</p>
                </div>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">Kontakt</h2>
                <p className="text-gray-300">
                  E-Mail: <a href="mailto:contact@basics-db.ch" className="text-green-400 hover:text-green-300 transition-colors">contact@basics-db.ch</a>
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">Unternehmensangaben</h2>
                <div className="text-gray-300">
                  <p>Handelsregisteramt des Kantons Zürich</p>
                  <p>UID: CHE-232.274.979</p>
                </div>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">Haftung & Inhalte</h2>
                <p className="text-gray-300 leading-relaxed">
                  Alle Inhalte wurden mit grösstmöglicher Sorgfalt erstellt. Für die Richtigkeit, Vollständigkeit und Aktualität übernehmen wir keine Gewähr. Verlinkte Inhalte Dritter liegen ausserhalb unseres Verantwortungsbereichs.
                </p>
              </section>

              <p className="text-gray-400 text-sm">
                * Hinweise zur Impressumspflicht in der Schweiz finden sich u. a. bei aktivMind/onlaw; für E-Commerce-Angebote sind klare Identitäts- und Kontaktangaben erforderlich (UWG Art. 3).
              </p>
            </div>
          </div>
        </div>
      </main>

      {/* Footer */}
      <footer className="bg-slate-900 py-8 sm:py-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex flex-col md:flex-row justify-between items-center gap-4">
            <div className="flex items-center gap-3 mb-4 md:mb-0">
              <a href="/" className="text-xl sm:text-2xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent hover:opacity-80 transition-opacity">SnapFare</a>
            </div>
            
            <div className="flex flex-col sm:flex-row items-center gap-4 text-center">
              <div className="flex gap-6 text-sm">
                <a href="/impressum" className="text-gray-400 hover:text-white transition-colors">
                  Impressum
                </a>
                <a href="/datenschutz" className="text-gray-400 hover:text-white transition-colors">
                  Datenschutzerklärung
                </a>
              </div>
              
              <p className="text-gray-400 text-xs sm:text-sm">
                © 2025 SnapFare. Alle Rechte vorbehalten.
              </p>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Impressum;
