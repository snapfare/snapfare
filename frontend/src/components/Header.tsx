import { Button } from "@/components/ui/button";
import { Link } from "react-router-dom";

const Header = () => {
  return (
    <header className="fixed top-0 left-0 right-0 z-50 bg-background/80 backdrop-blur-lg border-b border-border">
      <div className="container mx-auto px-6 py-4 flex items-center justify-between">
        <Link to="/" className="text-2xl font-bold bg-text-gradient bg-clip-text text-transparent">
          SnapFare
        </Link>
        
        <nav className="flex items-center gap-4">
          <Link to="/auth">
            <Button variant="outline" className="border-primary text-primary hover:bg-primary hover:text-primary-foreground font-semibold px-6">
              Anmelden
            </Button>
          </Link>
          <Link to="/premium">
            <Button variant="default" className="bg-primary hover:bg-primary/90 text-primary-foreground font-semibold px-6">
              Premium
            </Button>
          </Link>
        </nav>
      </div>
    </header>
  );
};

export default Header;