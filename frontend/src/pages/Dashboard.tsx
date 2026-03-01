import React, { useState, useEffect } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Calendar } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { useToast } from "@/hooks/use-toast";
import { supabase } from "@/integrations/supabase/client";
import { User, Session } from '@supabase/supabase-js';
import { useNavigate } from 'react-router-dom';
import { CalendarIcon, Plane, LogOut, User as UserIcon, ArrowLeft } from 'lucide-react';
import { format } from 'date-fns';
import { de } from 'date-fns/locale';
import { Link } from 'react-router-dom';

const Dashboard = () => {
  const [user, setUser] = useState<User | null>(null);
  const [session, setSession] = useState<Session | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const { toast } = useToast();
  const navigate = useNavigate();
  
  // Flight preferences state
  const [startDate, setStartDate] = useState<Date>();
  const [endDate, setEndDate] = useState<Date>();
  const [maxPrice, setMaxPrice] = useState('');
  const [travelClass, setTravelClass] = useState('economy');
  const [minDuration, setMinDuration] = useState('');
  const [maxDuration, setMaxDuration] = useState('');
  const [selectedCountries, setSelectedCountries] = useState<string[]>([]);

  const countries = [
    'Deutschland', 'Frankreich', 'Italien', 'Spanien', 'Portugal', 'Griechenland',
    'Kroatien', 'Türkei', 'USA', 'Kanada', 'Japan', 'Thailand', 'Indonesien',
    'Australien', 'Neuseeland', 'Brasilien', 'Argentinien', 'Chile', 'Südafrika',
    'Marokko', 'Ägypten', 'Vereinigte Arabische Emirate', 'Singapur', 'Malaysia',
    'Vietnam', 'Indien', 'Nepal', 'Island', 'Norwegen', 'Schweden', 'Dänemark',
    'Finnland', 'Polen', 'Tschechien', 'Ungarn', 'Österreich', 'Niederlande',
    'Belgien', 'Vereinigtes Königreich', 'Irland', 'Russland', 'China', 'Südkorea'
  ];

  useEffect(() => {
    // Set up auth state listener
    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      (event, session) => {
        setSession(session);
        setUser(session?.user ?? null);
        setIsLoading(false);
        
        // Redirect unauthenticated users to auth page
        if (!session?.user) {
          navigate('/auth');
        }
      }
    );

    // Check for existing session
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session);
      setUser(session?.user ?? null);
      setIsLoading(false);
      
      if (!session?.user) {
        navigate('/auth');
      }
    });

    return () => subscription.unsubscribe();
  }, [navigate]);

  const handleCountryChange = (country: string, checked: boolean) => {
    if (checked) {
      setSelectedCountries(prev => [...prev, country]);
    } else {
      setSelectedCountries(prev => prev.filter(c => c !== country));
    }
  };

  const handleSavePreferences = async () => {
    setIsSaving(true);
    
    try {
      // Hier würden wir später die Präferenzen in Supabase speichern
      // Für jetzt nur eine Bestätigung
      
      toast({
        title: "Präferenzen gespeichert!",
        description: "Ihre Flugpräferenzen wurden erfolgreich gespeichert.",
      });
      
      console.log('Flight Preferences:', {
        startDate,
        endDate,
        maxPrice,
        travelClass,
        minDuration,
        maxDuration,
        selectedCountries,
        userId: user?.id
      });
      
    } catch (error) {
      toast({
        title: "Fehler",
        description: "Beim Speichern der Präferenzen ist ein Fehler aufgetreten.",
        variant: "destructive",
      });
    } finally {
      setIsSaving(false);
    }
  };

  const handleSignOut = async () => {
    await supabase.auth.signOut();
    navigate('/');
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50">
        <div className="flex items-center justify-center min-h-screen">
          <div className="text-center text-gray-600">Laden...</div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-indigo-50 to-purple-50">
      <div className="container mx-auto px-6 py-8">
        {/* Back to Home Button */}
        <div className="mb-6">
          <Link to="/" className="inline-flex items-center gap-2 text-blue-600 hover:text-blue-800 font-medium">
            <ArrowLeft className="w-4 h-4" />
            Zurück zur Hauptseite
          </Link>
        </div>

        {/* User Info Header */}
        <div className="mb-8 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 bg-gradient-to-r from-blue-600 to-purple-600 rounded-full flex items-center justify-center">
              <UserIcon className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-2xl font-bold">Willkommen zurück!</h1>
              <p className="text-gray-600">{user?.email}</p>
            </div>
          </div>
          
          <Button variant="outline" onClick={handleSignOut} className="flex items-center gap-2 border-gray-300 text-gray-700 hover:bg-gray-50">
            <LogOut className="w-4 h-4" />
            Abmelden
          </Button>
        </div>

        {/* Flight Preferences */}
        <Card className="max-w-4xl mx-auto shadow-xl border-0 bg-white/90 backdrop-blur-sm">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-gray-800">
              <Plane className="w-6 h-6 text-blue-600" />
              Ihre Flugpräferenzen
            </CardTitle>
            <CardDescription className="text-gray-600">
              Geben Sie Ihre Reisepräferenzen an, um personalisierte Flugdeals zu erhalten.
            </CardDescription>
          </CardHeader>
          
          <CardContent className="space-y-6">
            {/* Travel Period */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label>Reisezeitraum - Von</Label>
                <Popover>
                  <PopoverTrigger asChild>
                    <Button
                      variant="outline"
                      className="w-full justify-start text-left font-normal"
                    >
                      <CalendarIcon className="mr-2 h-4 w-4" />
                      {startDate ? format(startDate, "PPP", { locale: de }) : "Startdatum wählen"}
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-auto p-0">
                    <Calendar
                      mode="single"
                      selected={startDate}
                      onSelect={setStartDate}
                      initialFocus
                    />
                  </PopoverContent>
                </Popover>
              </div>
              
              <div className="space-y-2">
                <Label>Reisezeitraum - Bis</Label>
                <Popover>
                  <PopoverTrigger asChild>
                    <Button
                      variant="outline"
                      className="w-full justify-start text-left font-normal"
                    >
                      <CalendarIcon className="mr-2 h-4 w-4" />
                      {endDate ? format(endDate, "PPP", { locale: de }) : "Enddatum wählen"}
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-auto p-0">
                    <Calendar
                      mode="single"
                      selected={endDate}
                      onSelect={setEndDate}
                      initialFocus
                    />
                  </PopoverContent>
                </Popover>
              </div>
            </div>

            {/* Max Price */}
            <div className="space-y-2">
              <Label htmlFor="maxPrice">Maximaler Preis pro Person (CHF)</Label>
              <Input
                id="maxPrice"
                type="number"
                placeholder="z.B. 800"
                value={maxPrice}
                onChange={(e) => setMaxPrice(e.target.value)}
              />
            </div>

            {/* Travel Class */}
            <div className="space-y-3">
              <Label>Reiseklasse</Label>
              <RadioGroup value={travelClass} onValueChange={setTravelClass}>
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="economy" id="economy" />
                  <Label htmlFor="economy">Economy</Label>
                </div>
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="business" id="business" />
                  <Label htmlFor="business">Business</Label>
                </div>
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="both" id="both" />
                  <Label htmlFor="both">Beide</Label>
                </div>
              </RadioGroup>
            </div>

            {/* Trip Duration */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="minDuration">Minimale Reisedauer (Tage)</Label>
                <Input
                  id="minDuration"
                  type="number"
                  placeholder="z.B. 3"
                  value={minDuration}
                  onChange={(e) => setMinDuration(e.target.value)}
                />
              </div>
              
              <div className="space-y-2">
                <Label htmlFor="maxDuration">Maximale Reisedauer (Tage)</Label>
                <Input
                  id="maxDuration"
                  type="number"
                  placeholder="z.B. 14"
                  value={maxDuration}
                  onChange={(e) => setMaxDuration(e.target.value)}
                />
              </div>
            </div>

            {/* Destinations */}
            <div className="space-y-3">
              <Label>Gewünschte Destinationen</Label>
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3 max-h-64 overflow-y-auto p-4 border rounded-md">
                {countries.map((country) => (
                  <div key={country} className="flex items-center space-x-2">
                    <Checkbox
                      id={country}
                      checked={selectedCountries.includes(country)}
                      onCheckedChange={(checked) => handleCountryChange(country, checked as boolean)}
                    />
                    <Label htmlFor={country} className="text-sm">{country}</Label>
                  </div>
                ))}
              </div>
              <p className="text-sm text-muted-foreground">
                {selectedCountries.length} {selectedCountries.length === 1 ? 'Land' : 'Länder'} ausgewählt
              </p>
            </div>

            {/* Save Button */}
            <div className="pt-6">
              <Button 
                onClick={handleSavePreferences}
                disabled={isSaving}
                className="w-full bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white font-semibold py-3"
                size="lg"
              >
                {isSaving ? "Speichern..." : "Präferenzen speichern"}
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Dashboard;