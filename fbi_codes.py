import us

state_code = us.states.mapping('fips', 'name')

state_abbr = us.states.mapping('abbr', 'name')

sex = {
       "M":"Male",
       "F":"Female",
       "U":"Unknown"
       }

victim_type = {
    "I":"Individual",
    "B":"Business",
    "F":"Financial Institution",
    "G":"Government",
    "R":"Religious Organization",
    "S":"Society/Public",
    "O":"Other",
    "U":"Unknown"
    }

weapon = {
    "11": "Firearm (type not stated)",
    "12":"Handgun",
    "13":"Rifle",
    "14":"Shotgun",
    "15":"Other Firearm",
    "16":"Lethal Cutting Instrument (e.g., switchblade knife, etc.)",
    "17":" Club/Blackjack/Brass Knuckles",
    "20":"Knife/Cutting Instrument (ice pick, screwdriver, ax, etc.)",
    "36": "Blunt Object (club, hammer, etc.)",
    "35": "Motor Vehicle",
    "40": "Personal Weapons (hands, feet, teeth, etc.)",
    "50": "Poison (include gas)",
    "60": "Explosives",
    "65":"Fire/Incendiary Device",
    "70":"Drugs/Narcotics/Sleeping Pills",
    "85":"Asphyxiation (by drowning, strangulation,suffocation, gas, etc.)",
    "90":"Other",
    "95":"Unknown",
    "99":"None"
    }

bias = {
        "11":"White",
        "12":"Black",
        "13":"American Indian or Alaskan Native",
        "14":"Asian/Pacific Islander",
        "15":"Multi-Racial Group",
        "21":"Jewish",
        "22":"Catholic",
        "23":"Protestant",
        "24":"Muslim",
        "25":"Other Religion",
        "26":"Multi-Religious Group",
        "27":"Atheism/Agnosticism",
        "31":"Arab",
        "32":"Hispanic",
        "33":"Other Ethnicity/Natl. Origin",
        "41":"Homosexual (Male)",
        "42":"Homosexual (Female)",
        "43":"Homosexual (Male and Female)",
        "44":"Heterosexual",
        "45":"Bisexual",
        "88":"None",
        "99":"Unknown"
        }

bias_group = {
        "11":"Anti_racial",
        "12":"Anti_racial",
        "13":"Anti_racial",
        "14":"Anti_racial",
        "15":"Anti_racial",
        "21":"Anti-religious",
        "22":"Anti-religious",
        "23":"Anti-religious",
        "24":"Anti-religious",
        "25":"Anti-religious",
        "26":"Anti-religious",
        "27":"Anti-religious",
        "31":"Anti Ethnicity/National Origin",
        "32":"Anti Ethnicity/National Origin",
        "33":"Anti Ethnicity/National Origin",
        "41":"Anti-Sexual",
        "42":"Anti-Sexual",
        "43":"Anti-Sexual",
        "44":"Anti-Sexual",
        "45":"Anti-Sexual",
        "88":"None",
        "99":"Unknown"
        }

location = {
    "01":"Air/Bus/Train Terminal",
    "02":"Bank:",
    "03": "Bar/Nightclub",
    "04":"Church/Synagogue/Temple",
    "05":"Commercial/Office Building",
    "06":"Construction Site",
    "07":"Convenience Store",
    "08":"Department/Discount Store",
    "09":"Drug Store/Dr.s Office/Hospital",
    "10": "Field/Woods",
    "11":"Government/Public Building",
    "12":"Grocery/Supermarket",
    "13":"Highway/Road/Alley",
    "14":"Hotel/Motel /Etc.",
    "15": "Jail/Prison",
    "16":"Lake/Waterway",
    "17":"Liquor Store",
    "18":"Parking Lot/Garage",
    "19":"Rental Stor. Facil.",
    "20":"Residence/Home",
    "21": "Restaurant",
    "22":"School/College",
    "23":"Service/Gas Stations",
    "24": "Specialty Store",
    "25":"Other/Unknown"
    }

agency = {
    "0":"Covered by Another Agency",
    "1":"City",
    "2":"County",
    "3":"University/College",
    "4":"State Police"
    }


division = {
    "0": "Territories",
    "1":"New England",
    "2":"Middle Atlantic",
    "3":"East North Central",
    "4":"West North Central",
    "5":"South Atlantic",
    "6":"East South Central",
    "7":"West South Central",
    "8":"Mountain",
    "9":"Pacific"
    }

region = {
    "1":"North East",
    "2":"North Central",
    "3":"South",
    "4":"West"
    }

offenses = {
    "200": "Arson",
    "13A": "Assault",
    "13B": "Assault",
    "13C": "Assault",
    "510": "Bribery",
    "220": "Burglary/Breaking and Entering",
    "250": "Counterfeiting/Forgery",
    "290":"Destruction/Damage/Vandalism",
    "35A":"Drug/Narcotic Offenses",
    "353": "Drug/Narcotic Offenses",
    "270": "Embezzlement",
    "210":"Extortion/Blackmail",
    "26A": "Fraud",
    "26B": "Fraud",
    "26C": "Fraud",
    "26D" :"Fraud",
    "26E":"Fraud",
    "39A": "Gambling Offenses",
    "39B":"Gambling Offenses",
    "39C":"Gambling Offenses",
    "39D":"Gambling Offenses",
    "09A":"Homicide",
    "09B":"Homicide",
    "09C":"Homicide",
    "100": "Kidnapping/Abduction",
    "23A":"Larceny/Theft",
    "23B":"Larceny/Theft",
    "23C":"Larceny/Theft",
    "23D":"Larceny/Theft",
    "23E":"Larceny/Theft",
    "23F":"Larceny/Theft",
    "23G":"Larceny/Theft",
    "23H":"Larceny/Theft",
    "240": "PROPERTY-Motor Vehicle Theft",
    "370":"SOCIETY ~Pornography/Obscene Material",
    "40A":"Prostitution Offenses",
    "40B": "Prostitution Offenses",
    "120":"Robbery",
    "11A":"Sex Offense- Forcible",
    "11B":"Sex Offense- Forcible",
    "11C":"Sex Offense- Forcible",
    "11D":"Sex Offense- Forcible",
    "36A": "Sex Offense - Non Forcible",
    "36B": "Sex Offense - Non Forcible",
    "280":"Stolen Property Offenses",
    "520":"Weapon Law Violations",
    "   ":"All offenses",
    "90A":"Bad Checks",
    "90B":"Curfew /Loitering/Vagrancy Violation",
    "90C":"Disorderly Conduct",
    "90D":"Driving Under the Influence",
    "90E":"Drunkenness",
    "90F":"Family Offenses, Nonviolent",
    "90G":"Liquor Law Violations",
    "90H":"Peeping Tom",
    "90I":"Runaway",
    "90J":"Trespass of Real Property",
    "90Z":"All Other Offenses"
    }

    