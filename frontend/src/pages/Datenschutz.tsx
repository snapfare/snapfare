const Datenschutz = () => {
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
      <main className="py-16 sm:py-20 lg:py-24">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="max-w-5xl mx-auto">
            <div className="bg-white/5 border border-white/10 rounded-2xl p-8 md:p-12">
              <h1 className="text-4xl font-bold mb-4 bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent">
                Datenschutzerklärung
              </h1>
              <p className="text-gray-300 text-sm mb-8">
                Gültig für Website, Newsletter (Free & Premium) und Bezahlvorgänge. Diese Erklärung erläutert Art, Umfang und Zwecke der Bearbeitung personenbezogener Daten nach schweizerischem Datenschutzrecht (revDSG) und – soweit anwendbar – der DSGVO.
              </p>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">1. Verantwortlicher</h2>
                <ul className="list-disc list-inside space-y-2 text-gray-300 leading-relaxed">
                  <li className="font-semibold">Basics by Dominic Bachmann (Einzelfirma)</li>
                  <li>Dominic Bachmann, 8123 Ebmatingen, Zürich, Schweiz</li>
                  <li>
                    E-Mail:{" "}
                    <a
                      href="mailto:contact@basics-db.ch"
                      className="text-green-400 hover:text-green-300 transition-colors"
                    >
                      contact@basics-db.ch
                    </a>
                  </li>
                </ul>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">2. Arten personenbezogener Daten</h2>
                <ul className="list-disc list-inside space-y-2 text-gray-300 leading-relaxed">
                  <li>
                    <strong>Stammdaten:</strong> Name, E-Mail (Newsletter-Anmeldung, Konto-Kommunikation)
                  </li>
                  <li>
                    <strong>Transaktionsdaten:</strong> Zahlungsstatus, Betrag, Zeitpunkt (über Zahlungsdienstleister verarbeitet; keine Speicherung von Karten-/Kontodaten bei uns)
                  </li>
                  <li>
                    <strong>Nutzungsdaten:</strong> Log-Daten, Geräte-/Browser-Infos, Newsletter-Öffnungen & Klicks
                  </li>
                  <li>
                    <strong>Cookies & Tracking:</strong> siehe Abschnitt <em>Cookies</em>
                  </li>
                </ul>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">3. Zwecke der Datenbearbeitung</h2>
                <ul className="list-disc list-inside space-y-2 text-gray-300 leading-relaxed">
                  <li>Bereitstellung der Website & technischer Betrieb</li>
                  <li>Versand des Newsletters, Verwaltung der Abonnements und Durchführung der Verlosungen</li>
                  <li>Abwicklung von Zahlungen (Premium)</li>
                  <li>Reichweitenmessung & Optimierung (Analytics)</li>
                  <li>Erfüllung rechtlicher Pflichten und Durchsetzung von Ansprüchen</li>
                </ul>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">4. Rechtsgrundlagen</h2>
                <p className="text-gray-300 leading-relaxed">
                  Nach revDSG: Bearbeitung erfolgt insbesondere zur Vertragserfüllung, aufgrund Einwilligung (Newsletter), berechtigter Interessen (Betrieb/Sicherheit/Statistik) und gesetzlicher Pflichten. Soweit die DSGVO anwendbar ist: Art. 6 Abs. 1 lit. a, b, c, f DSGVO.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">5. Empfänger & Auftragsbearbeiter (Drittanbieter)</h2>
                <ul className="list-disc list-inside space-y-2 text-gray-300 leading-relaxed">
                  <li><strong>Newsletter:</strong> EmailOctopus (Versand, Öffnungs-/Klickmessung)</li>
                  <li><strong>Datenbank/Backend:</strong> Supabase (Subscriber- und Payment-Daten; „Drittanbieter-Hosting")</li>
                  <li><strong>Zahlungen:</strong> TWINT Business und Payrexx (Abwicklung; Datenspeicherung auf sicheren Servern in CH/EU gemäss deren Datenschutzangaben)</li>
                  <li><strong>Analytics:</strong> Google Analytics (Web-Tracking; IP-Anonymisierung/verkürzte IP, Opt-Out-Möglichkeit)</li>
                  <li>Weitere Dienstleister im Rahmen von Support, Wartung und Hosting</li>
                </ul>
                <p className="text-gray-400 text-sm mt-4">
                  Hinweis: EDÖB betont transparente Datenschutzerklärungen; Zahlungsdienstleister wie TWINT/Payrexx verarbeiten Daten gemäss eigenen Richtlinien und CH/EU-Standards.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">6. Datenübermittlung ins Ausland</h2>
                <p className="text-gray-300 leading-relaxed">
                  Eine Übermittlung in andere Länder kann stattfinden, sofern dies für die Erbringung unserer Leistungen erforderlich ist oder gesetzliche Grundlagen bestehen. Dabei achten wir auf ein angemessenes Datenschutzniveau oder geeignete Garantien (z. B. Standardvertragsklauseln). Angaben der Zahlungsanbieter bestätigen Speicherung/Verarbeitung auf sicheren Servern in der Schweiz/EU.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">7. Cookies & Tracking</h2>
                <p className="text-gray-300 leading-relaxed mb-4">
                  Wir verwenden Cookies und ähnliche Technologien, um die Website bereitzustellen, Reichweiten zu messen und unsere Angebote zu verbessern. Beim ersten Besuch informieren wir über die Nutzung und bieten Einstellmöglichkeiten (Opt-In/Opt-Out je nach Kategorie). Die EDÖB-Leitlinien verlangen transparente Information und – je nach Zweck – Einwilligung für nicht technisch notwendige Cookies.
                </p>
                <h3 className="text-xl font-semibold mb-3 text-white">Google Analytics</h3>
                <p className="text-gray-300 leading-relaxed">
                  Wir setzen Google Analytics ein (IP-Anonymisierung/verkürzte IP). Dadurch werden Cookies gesetzt und Nutzungsdaten an Google übertragen, um aggregierte Statistiken zu erstellen. Du kannst die Erfassung jederzeit über unsere Cookie-Einstellungen oder das Google-Browser-Add-on deaktivieren.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">8. Newsletter (Free & Premium)</h2>
                <p className="text-gray-300 leading-relaxed">
                  Bei Anmeldung erfassen wir deine E-Mail (und optional deinen Namen). Der Versand erfolgt über EmailOctopus; Öffnungen und Klicks können gemessen werden, um Inhalte zu verbessern. Du kannst dich jederzeit über den Abmeldelink in jeder E-Mail austragen.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">9. Zahlungen (Premium)</h2>
                <p className="text-gray-300 leading-relaxed">
                  Zahlungen werden über <strong>TWINT Business</strong> und <strong>Payrexx</strong> abgewickelt. Zahlungsdaten werden dort verarbeitet; bei uns werden keine Kreditkarten- oder Kontodaten gespeichert. Die Anbieter betreiben Server in der Schweiz/EU und verarbeiten Personendaten nach geltendem Datenschutzrecht. Details findest du in den Datenschutzerklärungen der Anbieter.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">10. Speicherdauer</h2>
                <p className="text-gray-300 leading-relaxed">
                  Wir speichern Personendaten so lange, wie es für die genannten Zwecke erforderlich ist (z. B. laufendes Abonnement), gesetzliche Pflichten es verlangen (z. B. Aufbewahrung im Steuer-/Handelsrecht) oder berechtigte Interessen bestehen.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">11. Rechte der betroffenen Personen</h2>
                <p className="text-gray-300 leading-relaxed">
                  Du hast nach revDSG (und ggf. DSGVO) insbesondere das Recht auf Auskunft, Berichtigung, Löschung, Einschränkung der Bearbeitung, Datenübertragbarkeit (soweit anwendbar) sowie Widerruf von Einwilligungen. Wende dich hierzu an{" "}
                  <a href="mailto:contact@basics-db.ch" className="text-green-400 hover:text-green-300 transition-colors">
                    contact@basics-db.ch
                  </a>
                  . Zudem besteht das Recht, beim EDÖB Beschwerde zu erheben.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">12. Datensicherheit</h2>
                <p className="text-gray-300 leading-relaxed">
                  Wir treffen angemessene technische und organisatorische Massnahmen (TOM), um Personendaten vor Verlust, Missbrauch und unbefugtem Zugriff zu schützen. Zahlungs- und Transaktionsdaten werden über verschlüsselte Verbindungen übertragen; Zahlungsdienstleister setzen eigene Sicherheitsmassnahmen nach Branchenstandard um.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">13. Newsletter & Verlosungen</h2>
                <p className="text-gray-300 leading-relaxed">
                  Im Rahmen des Newsletters führen wir gelegentlich Verlosungen durch, z. B. von Premium-Mitgliedschaften. Die Teilnahme erfolgt automatisch mit einer aktiven Newsletter-Anmeldung. Unter allen gültigen Abonnentinnen und Abonnenten werden die Gewinnerinnen und Gewinner zufällig ausgelost und per E-Mail benachrichtigt. Die Teilnahme ist kostenlos, es besteht kein Kaufzwang. Keine Weitergabe an Dritte, ausser wenn dies für die Gewinnabwicklung erforderlich ist (z. B. Versand des Preises). Der Rechtsweg ist ausgeschlossen.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">14. Änderungen</h2>
                <p className="text-gray-300 leading-relaxed">
                  Wir können diese Datenschutzerklärung anpassen, wenn sich unsere Prozesse oder rechtliche Grundlagen ändern. Es gilt die jeweils auf dieser Seite veröffentlichte Version.
                </p>
              </section>

              <section className="mb-8">
                <h2 className="text-2xl font-semibold mb-4 text-white">Kontakt</h2>
                <p className="text-gray-300">
                  Fragen zu dieser Datenschutzerklärung:{" "}
                  <a href="mailto:contact@basics-db.ch" className="text-green-400 hover:text-green-300 transition-colors">
                    contact@basics-db.ch
                  </a>
                </p>
              </section>

              <p className="text-gray-400 text-sm">
                Rechtsgrundlagen & Leitlinien: EDÖB zu Datenschutzerklärungen/Cookies; Informationen zur Impressumspflicht s. CH-Praxis.
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
              <a
                href="/"
                className="text-xl sm:text-2xl font-bold bg-gradient-to-r from-green-400 to-blue-500 bg-clip-text text-transparent hover:opacity-80 transition-opacity"
              >
                SnapFare
              </a>
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

export default Datenschutz;
