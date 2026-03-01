import { Link } from "react-router-dom";

const Footer = () => {
  return (
    <footer className="bg-card border-t border-border py-12">
      <div className="container mx-auto px-6">
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-6">
          <div className="flex flex-col">
            <Link to="/" className="text-xl font-bold bg-text-gradient bg-clip-text text-transparent mb-2">
              SnapFare
            </Link>
            <p className="text-muted-foreground text-sm max-w-md">
              Deine vollautomatisierte Schnäppchenjagd für Flugdeals
            </p>
          </div>
          
          <div className="flex flex-col md:flex-row gap-6 text-sm">
            <Link 
              to="/impressum" 
              className="text-muted-foreground hover:text-foreground transition-colors"
            >
              Impressum
            </Link>
            <Link 
              to="/datenschutz" 
              className="text-muted-foreground hover:text-foreground transition-colors"
            >
              Datenschutzerklärung
            </Link>
          </div>
        </div>
        
        <div className="mt-8 pt-6 border-t border-border text-center text-xs text-muted-foreground">
          © 2024 SnapFare / Basics-DB. Alle Rechte vorbehalten.
        </div>
      </div>
    </footer>
  );
};

export default Footer;