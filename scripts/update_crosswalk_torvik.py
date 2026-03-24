"""
scripts/update_crosswalk_torvik.py

Merges Torvik team entries into data/crosswalks/cbb_teams.json.

Run from repo root:
    python scripts/update_crosswalk_torvik.py

What it does:
  1. Adds 253 new team entries covering all D-I teams Torvik serves
     that weren't in the original 112-team crosswalk.
  2. Merges Torvik-specific aliases into existing entries where the
     canonical name already exists (e.g. Wichita State, New Mexico).
  3. Writes the updated file in-place.
  4. Prints a summary of adds and merges.
"""

import json
import os
import sys

CROSSWALK_PATH = os.path.join(
    os.path.dirname(__file__), "../data/crosswalks/cbb_teams.json"
)

NEW_ENTRIES = [
    {"name": "Air Force", "aliases": ["Air Force Falcons"], "sources": {"torvik": "Air Force"}},
    {"name": "Akron", "aliases": ["Akron Zips"], "sources": {"torvik": "Akron"}},
    {"name": "Alabama A&M", "aliases": ["Alabama A&M Bulldogs"], "sources": {"torvik": "Alabama A&M"}},
    {"name": "Alabama State", "aliases": ["Alabama State Hornets", "Alabama St."], "sources": {"torvik": "Alabama St."}},
    {"name": "Albany", "aliases": ["Albany Great Danes", "UAlbany"], "sources": {"torvik": "Albany"}},
    {"name": "Alcorn State", "aliases": ["Alcorn State Braves", "Alcorn St."], "sources": {"torvik": "Alcorn St."}},
    {"name": "American", "aliases": ["American Eagles", "American University"], "sources": {"torvik": "American"}},
    {"name": "Appalachian State", "aliases": ["Appalachian State Mountaineers", "Appalachian St.", "App State"], "sources": {"torvik": "Appalachian St."}},
    {"name": "Arizona State", "aliases": ["Arizona State Sun Devils", "Arizona St."], "sources": {"torvik": "Arizona St."}},
    {"name": "Arkansas Pine Bluff", "aliases": ["Arkansas-Pine Bluff Golden Lions", "UAPB"], "sources": {"torvik": "Arkansas Pine Bluff"}},
    {"name": "Arkansas State", "aliases": ["Arkansas State Red Wolves", "Arkansas St."], "sources": {"torvik": "Arkansas St."}},
    {"name": "Army", "aliases": ["Army Black Knights", "Army West Point"], "sources": {"torvik": "Army"}},
    {"name": "Austin Peay", "aliases": ["Austin Peay Governors"], "sources": {"torvik": "Austin Peay"}},
    {"name": "Ball State", "aliases": ["Ball State Cardinals", "Ball St."], "sources": {"torvik": "Ball St."}},
    {"name": "Bellarmine", "aliases": ["Bellarmine Knights"], "sources": {"torvik": "Bellarmine"}},
    {"name": "Belmont", "aliases": ["Belmont Bruins"], "sources": {"torvik": "Belmont"}},
    {"name": "Bethune-Cookman", "aliases": ["Bethune Cookman Wildcats", "Bethune Cookman"], "sources": {"torvik": "Bethune Cookman"}},
    {"name": "Binghamton", "aliases": ["Binghamton Bearcats"], "sources": {"torvik": "Binghamton"}},
    {"name": "Boston College", "aliases": ["Boston College Eagles"], "sources": {"torvik": "Boston College"}},
    {"name": "Boston University", "aliases": ["Boston University Terriers", "BU"], "sources": {"torvik": "Boston University"}},
    {"name": "Bowling Green", "aliases": ["Bowling Green Falcons", "BGSU"], "sources": {"torvik": "Bowling Green"}},
    {"name": "Bradley", "aliases": ["Bradley Braves"], "sources": {"torvik": "Bradley"}},
    {"name": "Brown", "aliases": ["Brown Bears"], "sources": {"torvik": "Brown"}},
    {"name": "Bryant", "aliases": ["Bryant Bulldogs"], "sources": {"torvik": "Bryant"}},
    {"name": "Bucknell", "aliases": ["Bucknell Bison"], "sources": {"torvik": "Bucknell"}},
    {"name": "Buffalo", "aliases": ["Buffalo Bulls"], "sources": {"torvik": "Buffalo"}},
    {"name": "Butler", "aliases": ["Butler Bulldogs"], "sources": {"torvik": "Butler"}},
    {"name": "Cal Baptist", "aliases": ["California Baptist Lancers", "California Baptist", "CBU"], "sources": {"torvik": "Cal Baptist"}},
    {"name": "Cal Poly", "aliases": ["Cal Poly Mustangs", "California Polytechnic"], "sources": {"torvik": "Cal Poly"}},
    {"name": "Cal State Bakersfield", "aliases": ["Cal State Bakersfield Roadrunners", "Cal St. Bakersfield", "CSUB"], "sources": {"torvik": "Cal St. Bakersfield"}},
    {"name": "Cal State Fullerton", "aliases": ["Cal State Fullerton Titans", "Cal St. Fullerton", "CSUF"], "sources": {"torvik": "Cal St. Fullerton"}},
    {"name": "Cal State Northridge", "aliases": ["Cal State Northridge Matadors", "Cal St. Northridge", "CSUN"], "sources": {"torvik": "Cal St. Northridge"}},
    {"name": "California", "aliases": ["California Golden Bears", "Cal"], "sources": {"torvik": "California"}},
    {"name": "Campbell", "aliases": ["Campbell Fighting Camels"], "sources": {"torvik": "Campbell"}},
    {"name": "Canisius", "aliases": ["Canisius Golden Griffins"], "sources": {"torvik": "Canisius"}},
    {"name": "Central Arkansas", "aliases": ["Central Arkansas Bears", "UCA"], "sources": {"torvik": "Central Arkansas"}},
    {"name": "Central Connecticut", "aliases": ["Central Connecticut State Blue Devils", "CCSU"], "sources": {"torvik": "Central Connecticut"}},
    {"name": "Central Michigan", "aliases": ["Central Michigan Chippewas"], "sources": {"torvik": "Central Michigan"}},
    {"name": "Charleston Southern", "aliases": ["Charleston Southern Buccaneers", "CSU"], "sources": {"torvik": "Charleston Southern"}},
    {"name": "Charlotte", "aliases": ["Charlotte 49ers", "UNC Charlotte"], "sources": {"torvik": "Charlotte"}},
    {"name": "Chattanooga", "aliases": ["Chattanooga Mocs", "UTC"], "sources": {"torvik": "Chattanooga"}},
    {"name": "Chicago State", "aliases": ["Chicago State Cougars", "Chicago St."], "sources": {"torvik": "Chicago St."}},
    {"name": "Cincinnati", "aliases": ["Cincinnati Bearcats"], "sources": {"torvik": "Cincinnati"}},
    {"name": "Cleveland State", "aliases": ["Cleveland State Vikings", "Cleveland St."], "sources": {"torvik": "Cleveland St."}},
    {"name": "Coastal Carolina", "aliases": ["Coastal Carolina Chanticleers"], "sources": {"torvik": "Coastal Carolina"}},
    {"name": "Columbia", "aliases": ["Columbia Lions"], "sources": {"torvik": "Columbia"}},
    {"name": "Coppin State", "aliases": ["Coppin State Eagles", "Coppin St."], "sources": {"torvik": "Coppin St."}},
    {"name": "Cornell", "aliases": ["Cornell Big Red"], "sources": {"torvik": "Cornell"}},
    {"name": "Dartmouth", "aliases": ["Dartmouth Big Green"], "sources": {"torvik": "Dartmouth"}},
    {"name": "DePaul", "aliases": ["DePaul Blue Demons"], "sources": {"torvik": "DePaul"}},
    {"name": "Delaware", "aliases": ["Delaware Fightin Blue Hens"], "sources": {"torvik": "Delaware"}},
    {"name": "Delaware State", "aliases": ["Delaware State Hornets", "Delaware St."], "sources": {"torvik": "Delaware St."}},
    {"name": "Denver", "aliases": ["Denver Pioneers"], "sources": {"torvik": "Denver"}},
    {"name": "Detroit Mercy", "aliases": ["Detroit Mercy Titans"], "sources": {"torvik": "Detroit Mercy"}},
    {"name": "Drexel", "aliases": ["Drexel Dragons"], "sources": {"torvik": "Drexel"}},
    {"name": "Duquesne", "aliases": ["Duquesne Dukes"], "sources": {"torvik": "Duquesne"}},
    {"name": "East Carolina", "aliases": ["East Carolina Pirates", "ECU"], "sources": {"torvik": "East Carolina"}},
    {"name": "East Tennessee State", "aliases": ["East Tennessee State Buccaneers", "East Tennessee St.", "ETSU"], "sources": {"torvik": "East Tennessee St."}},
    {"name": "East Texas A&M", "aliases": ["East Texas A&M Lions"], "sources": {"torvik": "East Texas A&M"}},
    {"name": "Eastern Illinois", "aliases": ["Eastern Illinois Panthers", "EIU"], "sources": {"torvik": "Eastern Illinois"}},
    {"name": "Eastern Kentucky", "aliases": ["Eastern Kentucky Colonels", "EKU"], "sources": {"torvik": "Eastern Kentucky"}},
    {"name": "Eastern Michigan", "aliases": ["Eastern Michigan Eagles", "EMU"], "sources": {"torvik": "Eastern Michigan"}},
    {"name": "Eastern Washington", "aliases": ["Eastern Washington Eagles", "EWU"], "sources": {"torvik": "Eastern Washington"}},
    {"name": "Elon", "aliases": ["Elon Phoenix"], "sources": {"torvik": "Elon"}},
    {"name": "Evansville", "aliases": ["Evansville Purple Aces"], "sources": {"torvik": "Evansville"}},
    {"name": "FIU", "aliases": ["FIU Panthers", "Florida International"], "sources": {"torvik": "FIU"}},
    {"name": "Fairfield", "aliases": ["Fairfield Stags"], "sources": {"torvik": "Fairfield"}},
    {"name": "Florida A&M", "aliases": ["Florida A&M Rattlers", "FAMU"], "sources": {"torvik": "Florida A&M"}},
    {"name": "Florida Gulf Coast", "aliases": ["Florida Gulf Coast Eagles", "FGCU"], "sources": {"torvik": "Florida Gulf Coast"}},
    {"name": "Fordham", "aliases": ["Fordham Rams"], "sources": {"torvik": "Fordham"}},
    {"name": "Fresno State", "aliases": ["Fresno State Bulldogs", "Fresno St."], "sources": {"torvik": "Fresno St."}},
    {"name": "Gardner-Webb", "aliases": ["Gardner-Webb Runnin Bulldogs", "Gardner Webb"], "sources": {"torvik": "Gardner Webb"}},
    {"name": "George Mason", "aliases": ["George Mason Patriots"], "sources": {"torvik": "George Mason"}},
    {"name": "George Washington", "aliases": ["George Washington Colonials", "GW"], "sources": {"torvik": "George Washington"}},
    {"name": "Georgia Southern", "aliases": ["Georgia Southern Eagles"], "sources": {"torvik": "Georgia Southern"}},
    {"name": "Georgia State", "aliases": ["Georgia State Panthers", "Georgia St."], "sources": {"torvik": "Georgia St."}},
    {"name": "Georgia Tech", "aliases": ["Georgia Tech Yellow Jackets"], "sources": {"torvik": "Georgia Tech"}},
    {"name": "Green Bay", "aliases": ["Green Bay Phoenix", "Wisconsin-Green Bay"], "sources": {"torvik": "Green Bay"}},
    {"name": "Hampton", "aliases": ["Hampton Pirates"], "sources": {"torvik": "Hampton"}},
    {"name": "Harvard", "aliases": ["Harvard Crimson"], "sources": {"torvik": "Harvard"}},
    {"name": "Hawaii", "aliases": ["Hawaii Rainbow Warriors", "Hawai'i Rainbow Warriors"], "sources": {"torvik": "Hawaii"}},
    {"name": "High Point", "aliases": ["High Point Panthers"], "sources": {"torvik": "High Point"}},
    {"name": "Hofstra", "aliases": ["Hofstra Pride"], "sources": {"torvik": "Hofstra"}},
    {"name": "Holy Cross", "aliases": ["Holy Cross Crusaders"], "sources": {"torvik": "Holy Cross"}},
    {"name": "Houston Christian", "aliases": ["Houston Christian Huskies", "HCU"], "sources": {"torvik": "Houston Christian"}},
    {"name": "IU Indianapolis", "aliases": ["IU Indianapolis Jaguars", "IUPUI", "IU Indy"], "sources": {"torvik": "IU Indy"}},
    {"name": "Idaho", "aliases": ["Idaho Vandals"], "sources": {"torvik": "Idaho"}},
    {"name": "Idaho State", "aliases": ["Idaho State Bengals", "Idaho St."], "sources": {"torvik": "Idaho St."}},
    {"name": "Illinois Chicago", "aliases": ["UIC Flames", "UIC", "Illinois-Chicago"], "sources": {"torvik": "Illinois Chicago"}},
    {"name": "Illinois State", "aliases": ["Illinois State Redbirds", "Illinois St."], "sources": {"torvik": "Illinois St."}},
    {"name": "Incarnate Word", "aliases": ["Incarnate Word Cardinals", "UIW"], "sources": {"torvik": "Incarnate Word"}},
    {"name": "Indiana State", "aliases": ["Indiana State Sycamores", "Indiana St."], "sources": {"torvik": "Indiana St."}},
    {"name": "Jackson State", "aliases": ["Jackson State Tigers", "Jackson St."], "sources": {"torvik": "Jackson St."}},
    {"name": "Jacksonville", "aliases": ["Jacksonville Dolphins"], "sources": {"torvik": "Jacksonville"}},
    {"name": "Jacksonville State", "aliases": ["Jacksonville State Gamecocks", "Jacksonville St."], "sources": {"torvik": "Jacksonville St."}},
    {"name": "Kennesaw State", "aliases": ["Kennesaw State Owls", "Kennesaw St."], "sources": {"torvik": "Kennesaw St."}},
    {"name": "Kent State", "aliases": ["Kent State Golden Flashes", "Kent St."], "sources": {"torvik": "Kent St."}},
    {"name": "LIU", "aliases": ["LIU Sharks", "Long Island University"], "sources": {"torvik": "LIU"}},
    {"name": "La Salle", "aliases": ["La Salle Explorers"], "sources": {"torvik": "La Salle"}},
    {"name": "Lafayette", "aliases": ["Lafayette Leopards"], "sources": {"torvik": "Lafayette"}},
    {"name": "Lamar", "aliases": ["Lamar Cardinals"], "sources": {"torvik": "Lamar"}},
    {"name": "Le Moyne", "aliases": ["Le Moyne Dolphins"], "sources": {"torvik": "Le Moyne"}},
    {"name": "Lehigh", "aliases": ["Lehigh Mountain Hawks"], "sources": {"torvik": "Lehigh"}},
    {"name": "Lindenwood", "aliases": ["Lindenwood Lions"], "sources": {"torvik": "Lindenwood"}},
    {"name": "Lipscomb", "aliases": ["Lipscomb Bisons"], "sources": {"torvik": "Lipscomb"}},
    {"name": "Little Rock", "aliases": ["Little Rock Trojans", "Arkansas-Little Rock", "UALR"], "sources": {"torvik": "Little Rock"}},
    {"name": "Long Beach State", "aliases": ["Long Beach State Beach", "Long Beach St.", "LBSU"], "sources": {"torvik": "Long Beach St."}},
    {"name": "Louisiana", "aliases": ["Louisiana Ragin Cajuns", "UL Lafayette", "ULL"], "sources": {"torvik": "Louisiana"}},
    {"name": "Louisiana Monroe", "aliases": ["Louisiana Monroe Warhawks", "ULM"], "sources": {"torvik": "Louisiana Monroe"}},
    {"name": "Louisiana Tech", "aliases": ["Louisiana Tech Bulldogs", "LA Tech"], "sources": {"torvik": "Louisiana Tech"}},
    {"name": "Louisville", "aliases": ["Louisville Cardinals"], "sources": {"torvik": "Louisville"}},
    {"name": "Loyola Maryland", "aliases": ["Loyola Maryland Greyhounds", "Loyola MD"], "sources": {"torvik": "Loyola MD"}},
    {"name": "Loyola Marymount", "aliases": ["Loyola Marymount Lions", "LMU"], "sources": {"torvik": "Loyola Marymount"}},
    {"name": "Maine", "aliases": ["Maine Black Bears"], "sources": {"torvik": "Maine"}},
    {"name": "Manhattan", "aliases": ["Manhattan Jaspers"], "sources": {"torvik": "Manhattan"}},
    {"name": "Marist", "aliases": ["Marist Red Foxes"], "sources": {"torvik": "Marist"}},
    {"name": "Marshall", "aliases": ["Marshall Thundering Herd"], "sources": {"torvik": "Marshall"}},
    {"name": "Maryland Eastern Shore", "aliases": ["Maryland-Eastern Shore Hawks", "UMES"], "sources": {"torvik": "Maryland Eastern Shore"}},
    {"name": "Massachusetts", "aliases": ["Massachusetts Minutemen", "UMass"], "sources": {"torvik": "Massachusetts"}},
    {"name": "Mercer", "aliases": ["Mercer Bears"], "sources": {"torvik": "Mercer"}},
    {"name": "Mercyhurst", "aliases": ["Mercyhurst Lakers"], "sources": {"torvik": "Mercyhurst"}},
    {"name": "Merrimack", "aliases": ["Merrimack Warriors"], "sources": {"torvik": "Merrimack"}},
    {"name": "Middle Tennessee", "aliases": ["Middle Tennessee Blue Raiders", "MTSU"], "sources": {"torvik": "Middle Tennessee"}},
    {"name": "Milwaukee", "aliases": ["Milwaukee Panthers", "Wisconsin-Milwaukee"], "sources": {"torvik": "Milwaukee"}},
    {"name": "Minnesota", "aliases": ["Minnesota Golden Gophers"], "sources": {"torvik": "Minnesota"}},
    {"name": "Mississippi Valley State", "aliases": ["Mississippi Valley State Delta Devils", "Mississippi Valley St.", "MVSU"], "sources": {"torvik": "Mississippi Valley St."}},
    {"name": "Missouri State", "aliases": ["Missouri State Bears", "Missouri St."], "sources": {"torvik": "Missouri St."}},
    {"name": "Monmouth", "aliases": ["Monmouth Hawks"], "sources": {"torvik": "Monmouth"}},
    {"name": "Montana", "aliases": ["Montana Grizzlies"], "sources": {"torvik": "Montana"}},
    {"name": "Montana State", "aliases": ["Montana State Bobcats", "Montana St."], "sources": {"torvik": "Montana St."}},
    {"name": "Morgan State", "aliases": ["Morgan State Bears", "Morgan St."], "sources": {"torvik": "Morgan St."}},
    {"name": "Mount St. Mary's", "aliases": ["Mount St. Mary's Mountaineers"], "sources": {"torvik": "Mount St. Mary's"}},
    {"name": "NJIT", "aliases": ["NJIT Highlanders", "New Jersey Institute of Technology"], "sources": {"torvik": "NJIT"}},
    {"name": "Navy", "aliases": ["Navy Midshipmen"], "sources": {"torvik": "Navy"}},
    {"name": "Nebraska Omaha", "aliases": ["Omaha Mavericks", "UNO"], "sources": {"torvik": "Nebraska Omaha"}},
    {"name": "New Hampshire", "aliases": ["New Hampshire Wildcats", "UNH"], "sources": {"torvik": "New Hampshire"}},
    {"name": "New Haven", "aliases": ["New Haven Chargers"], "sources": {"torvik": "New Haven"}},
    {"name": "New Mexico State", "aliases": ["New Mexico State Aggies", "New Mexico St.", "NMSU"], "sources": {"torvik": "New Mexico St."}},
    {"name": "New Orleans", "aliases": ["New Orleans Privateers", "UNO Privateers"], "sources": {"torvik": "New Orleans"}},
    {"name": "Niagara", "aliases": ["Niagara Purple Eagles"], "sources": {"torvik": "Niagara"}},
    {"name": "Nicholls State", "aliases": ["Nicholls State Colonels", "Nicholls St.", "Nicholls"], "sources": {"torvik": "Nicholls St."}},
    {"name": "Norfolk State", "aliases": ["Norfolk State Spartans", "Norfolk St."], "sources": {"torvik": "Norfolk St."}},
    {"name": "North Alabama", "aliases": ["North Alabama Lions", "UNA"], "sources": {"torvik": "North Alabama"}},
    {"name": "North Carolina A&T", "aliases": ["North Carolina A&T Aggies", "NC A&T"], "sources": {"torvik": "North Carolina A&T"}},
    {"name": "North Carolina Central", "aliases": ["North Carolina Central Eagles", "NCCU"], "sources": {"torvik": "North Carolina Central"}},
    {"name": "North Dakota", "aliases": ["North Dakota Fighting Hawks", "UND"], "sources": {"torvik": "North Dakota"}},
    {"name": "North Dakota State", "aliases": ["North Dakota State Bison", "North Dakota St.", "NDSU"], "sources": {"torvik": "North Dakota St."}},
    {"name": "North Florida", "aliases": ["North Florida Ospreys", "UNF"], "sources": {"torvik": "North Florida"}},
    {"name": "North Texas", "aliases": ["North Texas Mean Green", "UNT"], "sources": {"torvik": "North Texas"}},
    {"name": "Northeastern", "aliases": ["Northeastern Huskies"], "sources": {"torvik": "Northeastern"}},
    {"name": "Northern Arizona", "aliases": ["Northern Arizona Lumberjacks", "NAU"], "sources": {"torvik": "Northern Arizona"}},
    {"name": "Northern Colorado", "aliases": ["Northern Colorado Bears"], "sources": {"torvik": "Northern Colorado"}},
    {"name": "Northern Illinois", "aliases": ["Northern Illinois Huskies", "NIU"], "sources": {"torvik": "Northern Illinois"}},
    {"name": "Northern Iowa", "aliases": ["Northern Iowa Panthers", "UNI"], "sources": {"torvik": "Northern Iowa"}},
    {"name": "Northern Kentucky", "aliases": ["Northern Kentucky Norse", "NKU"], "sources": {"torvik": "Northern Kentucky"}},
    {"name": "Northwestern State", "aliases": ["Northwestern State Demons", "Northwestern St."], "sources": {"torvik": "Northwestern St."}},
    {"name": "Notre Dame", "aliases": ["Notre Dame Fighting Irish"], "sources": {"torvik": "Notre Dame"}},
    {"name": "Ohio", "aliases": ["Ohio Bobcats"], "sources": {"torvik": "Ohio"}},
    {"name": "Old Dominion", "aliases": ["Old Dominion Monarchs", "ODU"], "sources": {"torvik": "Old Dominion"}},
    {"name": "Pacific", "aliases": ["Pacific Tigers"], "sources": {"torvik": "Pacific"}},
    {"name": "Penn", "aliases": ["Pennsylvania Quakers", "Pennsylvania"], "sources": {"torvik": "Penn"}},
    {"name": "Pepperdine", "aliases": ["Pepperdine Waves"], "sources": {"torvik": "Pepperdine"}},
    {"name": "Portland", "aliases": ["Portland Pilots"], "sources": {"torvik": "Portland"}},
    {"name": "Portland State", "aliases": ["Portland State Vikings", "Portland St."], "sources": {"torvik": "Portland St."}},
    {"name": "Prairie View A&M", "aliases": ["Prairie View A&M Panthers", "PVAMU"], "sources": {"torvik": "Prairie View A&M"}},
    {"name": "Presbyterian", "aliases": ["Presbyterian Blue Hose"], "sources": {"torvik": "Presbyterian"}},
    {"name": "Purdue Fort Wayne", "aliases": ["Purdue Fort Wayne Mastodons", "PFW"], "sources": {"torvik": "Purdue Fort Wayne"}},
    {"name": "Queens", "aliases": ["Queens Royals"], "sources": {"torvik": "Queens"}},
    {"name": "Quinnipiac", "aliases": ["Quinnipiac Bobcats"], "sources": {"torvik": "Quinnipiac"}},
    {"name": "Radford", "aliases": ["Radford Highlanders"], "sources": {"torvik": "Radford"}},
    {"name": "Rhode Island", "aliases": ["Rhode Island Rams", "URI"], "sources": {"torvik": "Rhode Island"}},
    {"name": "Rice", "aliases": ["Rice Owls"], "sources": {"torvik": "Rice"}},
    {"name": "Rider", "aliases": ["Rider Broncs"], "sources": {"torvik": "Rider"}},
    {"name": "Robert Morris", "aliases": ["Robert Morris Colonials"], "sources": {"torvik": "Robert Morris"}},
    {"name": "SIU Edwardsville", "aliases": ["SIU Edwardsville Cougars", "SIUE"], "sources": {"torvik": "SIU Edwardsville"}},
    {"name": "SMU", "aliases": ["SMU Mustangs", "Southern Methodist"], "sources": {"torvik": "SMU"}},
    {"name": "Sacramento State", "aliases": ["Sacramento State Hornets", "Sacramento St.", "Sac State"], "sources": {"torvik": "Sacramento St."}},
    {"name": "Sacred Heart", "aliases": ["Sacred Heart Pioneers"], "sources": {"torvik": "Sacred Heart"}},
    {"name": "Saint Francis", "aliases": ["Saint Francis Red Flash", "Saint Francis (PA)", "SFU"], "sources": {"torvik": "Saint Francis"}},
    {"name": "Saint Joseph's", "aliases": ["Saint Joseph's Hawks", "Saint Joseph's(A)", "St. Joseph's"], "sources": {"torvik": "Saint Joseph's(A)"}},
    {"name": "Sam Houston", "aliases": ["Sam Houston Bearkats", "Sam Houston St.", "SHSU"], "sources": {"torvik": "Sam Houston St."}},
    {"name": "San Diego", "aliases": ["San Diego Toreros", "USD"], "sources": {"torvik": "San Diego"}},
    {"name": "San Jose State", "aliases": ["San Jose State Spartans", "San Jose St.", "SJSU"], "sources": {"torvik": "San Jose St."}},
    {"name": "Santa Clara", "aliases": ["Santa Clara Broncos"], "sources": {"torvik": "Santa Clara"}},
    {"name": "Seattle", "aliases": ["Seattle Redhawks", "Seattle University"], "sources": {"torvik": "Seattle"}},
    {"name": "Siena", "aliases": ["Siena Saints"], "sources": {"torvik": "Siena"}},
    {"name": "South Alabama", "aliases": ["South Alabama Jaguars"], "sources": {"torvik": "South Alabama"}},
    {"name": "South Carolina State", "aliases": ["South Carolina State Bulldogs", "South Carolina St.", "SC State"], "sources": {"torvik": "South Carolina St."}},
    {"name": "South Dakota", "aliases": ["South Dakota Coyotes"], "sources": {"torvik": "South Dakota"}},
    {"name": "South Florida", "aliases": ["South Florida Bulls", "USF"], "sources": {"torvik": "South Florida"}},
    {"name": "Southeast Missouri State", "aliases": ["Southeast Missouri State Redhawks", "Southeast Missouri St.", "SEMO"], "sources": {"torvik": "Southeast Missouri St."}},
    {"name": "Southeastern Louisiana", "aliases": ["Southeastern Louisiana Lions"], "sources": {"torvik": "Southeastern Louisiana"}},
    {"name": "Southern", "aliases": ["Southern Jaguars", "Southern University"], "sources": {"torvik": "Southern"}},
    {"name": "Southern Illinois", "aliases": ["Southern Illinois Salukis", "SIU"], "sources": {"torvik": "Southern Illinois"}},
    {"name": "Southern Indiana", "aliases": ["Southern Indiana Screaming Eagles", "USI"], "sources": {"torvik": "Southern Indiana"}},
    {"name": "Southern Miss", "aliases": ["Southern Miss Golden Eagles", "USM"], "sources": {"torvik": "Southern Miss"}},
    {"name": "Southern Utah", "aliases": ["Southern Utah Thunderbirds", "SUU"], "sources": {"torvik": "Southern Utah"}},
    {"name": "St. Thomas", "aliases": ["St. Thomas Tommies", "Saint Thomas"], "sources": {"torvik": "St. Thomas"}},
    {"name": "Stanford", "aliases": ["Stanford Cardinal"], "sources": {"torvik": "Stanford"}},
    {"name": "Stephen F. Austin", "aliases": ["Stephen F. Austin Lumberjacks", "SFA"], "sources": {"torvik": "Stephen F. Austin"}},
    {"name": "Stetson", "aliases": ["Stetson Hatters"], "sources": {"torvik": "Stetson"}},
    {"name": "Stonehill", "aliases": ["Stonehill Skyhawks"], "sources": {"torvik": "Stonehill"}},
    {"name": "Stony Brook", "aliases": ["Stony Brook Seawolves"], "sources": {"torvik": "Stony Brook"}},
    {"name": "Syracuse", "aliases": ["Syracuse Orange"], "sources": {"torvik": "Syracuse"}},
    {"name": "Tarleton State", "aliases": ["Tarleton State Texans", "Tarleton St."], "sources": {"torvik": "Tarleton St."}},
    {"name": "Temple", "aliases": ["Temple Owls"], "sources": {"torvik": "Temple"}},
    {"name": "Tennessee Martin", "aliases": ["Tennessee-Martin Skyhawks", "UT Martin"], "sources": {"torvik": "Tennessee Martin"}},
    {"name": "Tennessee State", "aliases": ["Tennessee State Tigers", "Tennessee St."], "sources": {"torvik": "Tennessee St."}},
    {"name": "Tennessee Tech", "aliases": ["Tennessee Tech Golden Eagles"], "sources": {"torvik": "Tennessee Tech"}},
    {"name": "Texas A&M Corpus Christi", "aliases": ["Texas A&M-Corpus Christi Islanders", "Texas A&M Corpus Chris"], "sources": {"torvik": "Texas A&M Corpus Chris"}},
    {"name": "Texas Southern", "aliases": ["Texas Southern Tigers", "TSU"], "sources": {"torvik": "Texas Southern"}},
    {"name": "Texas State", "aliases": ["Texas State Bobcats", "Texas St."], "sources": {"torvik": "Texas St."}},
    {"name": "The Citadel", "aliases": ["The Citadel Bulldogs", "Citadel"], "sources": {"torvik": "The Citadel"}},
    {"name": "Toledo", "aliases": ["Toledo Rockets"], "sources": {"torvik": "Toledo"}},
    {"name": "Towson", "aliases": ["Towson Tigers"], "sources": {"torvik": "Towson"}},
    {"name": "Troy", "aliases": ["Troy Trojans"], "sources": {"torvik": "Troy"}},
    {"name": "Tulane", "aliases": ["Tulane Green Wave"], "sources": {"torvik": "Tulane"}},
    {"name": "Tulsa", "aliases": ["Tulsa Golden Hurricane", "Tulsa(H)"], "sources": {"torvik": "Tulsa(H)"}},
    {"name": "UAB", "aliases": ["UAB Blazers", "Alabama-Birmingham"], "sources": {"torvik": "UAB"}},
    {"name": "UC Davis", "aliases": ["UC Davis Aggies"], "sources": {"torvik": "UC Davis"}},
    {"name": "UC Irvine", "aliases": ["UC Irvine Anteaters", "UCI"], "sources": {"torvik": "UC Irvine"}},
    {"name": "UC Riverside", "aliases": ["UC Riverside Highlanders", "UCR"], "sources": {"torvik": "UC Riverside"}},
    {"name": "UC San Diego", "aliases": ["UC San Diego Tritons", "UCSD"], "sources": {"torvik": "UC San Diego"}},
    {"name": "UCF", "aliases": ["UCF Knights", "Central Florida"], "sources": {"torvik": "UCF"}},
    {"name": "UMBC", "aliases": ["UMBC Retrievers", "Maryland-Baltimore County"], "sources": {"torvik": "UMBC"}},
    {"name": "UMKC", "aliases": ["Kansas City Roos", "Missouri-Kansas City"], "sources": {"torvik": "UMKC"}},
    {"name": "UMass Lowell", "aliases": ["UMass Lowell River Hawks"], "sources": {"torvik": "UMass Lowell"}},
    {"name": "UNC Asheville", "aliases": ["UNC Asheville Bulldogs"], "sources": {"torvik": "UNC Asheville"}},
    {"name": "UNC Greensboro", "aliases": ["UNC Greensboro Spartans", "UNCG"], "sources": {"torvik": "UNC Greensboro"}},
    {"name": "UNC Wilmington", "aliases": ["UNC Wilmington Seahawks", "UNCW"], "sources": {"torvik": "UNC Wilmington"}},
    {"name": "UNLV", "aliases": ["UNLV Rebels"], "sources": {"torvik": "UNLV"}},
    {"name": "USC Upstate", "aliases": ["USC Upstate Spartans"], "sources": {"torvik": "USC Upstate"}},
    {"name": "UT Arlington", "aliases": ["UT Arlington Mavericks", "UTA"], "sources": {"torvik": "UT Arlington"}},
    {"name": "UT Rio Grande Valley", "aliases": ["UT Rio Grande Valley Vaqueros", "UTRGV"], "sources": {"torvik": "UT Rio Grande Valley"}},
    {"name": "UTEP", "aliases": ["UTEP Miners", "Texas-El Paso"], "sources": {"torvik": "UTEP"}},
    {"name": "UTSA", "aliases": ["UTSA Roadrunners", "Texas-San Antonio"], "sources": {"torvik": "UTSA"}},
    {"name": "Utah", "aliases": ["Utah Utes"], "sources": {"torvik": "Utah"}},
    {"name": "Utah Tech", "aliases": ["Utah Tech Trailblazers", "Dixie State"], "sources": {"torvik": "Utah Tech"}},
    {"name": "Utah Valley", "aliases": ["Utah Valley Wolverines", "UVU"], "sources": {"torvik": "Utah Valley"}},
    {"name": "VMI", "aliases": ["VMI Keydets", "Virginia Military Institute"], "sources": {"torvik": "VMI"}},
    {"name": "Valparaiso", "aliases": ["Valparaiso Beacons"], "sources": {"torvik": "Valparaiso"}},
    {"name": "Virginia Tech", "aliases": ["Virginia Tech Hokies"], "sources": {"torvik": "Virginia Tech"}},
    {"name": "Washington", "aliases": ["Washington Huskies"], "sources": {"torvik": "Washington"}},
    {"name": "Weber State", "aliases": ["Weber State Wildcats", "Weber St."], "sources": {"torvik": "Weber St."}},
    {"name": "West Georgia", "aliases": ["West Georgia Wolves"], "sources": {"torvik": "West Georgia"}},
    {"name": "Western Carolina", "aliases": ["Western Carolina Catamounts"], "sources": {"torvik": "Western Carolina"}},
    {"name": "Western Illinois", "aliases": ["Western Illinois Leathernecks"], "sources": {"torvik": "Western Illinois"}},
    {"name": "Western Kentucky", "aliases": ["Western Kentucky Hilltoppers", "WKU"], "sources": {"torvik": "Western Kentucky"}},
    {"name": "Western Michigan", "aliases": ["Western Michigan Broncos", "WMU"], "sources": {"torvik": "Western Michigan"}},
    {"name": "William & Mary", "aliases": ["William & Mary Tribe"], "sources": {"torvik": "William & Mary"}},
    {"name": "Wofford", "aliases": ["Wofford Terriers"], "sources": {"torvik": "Wofford"}},
    {"name": "Wright State", "aliases": ["Wright State Raiders", "Wright St."], "sources": {"torvik": "Wright St."}},
    {"name": "Youngstown State", "aliases": ["Youngstown State Penguins", "Youngstown St."], "sources": {"torvik": "Youngstown St."}},
]

# Entries that already exist in the crosswalk but need Torvik aliases merged in
MERGE_INTO_EXISTING = {
    # canonical name -> {aliases to add, sources to add}
    "Wichita State": {
        "aliases": ["Wichita St.(A)"],
        "sources": {"torvik": "Wichita St.(A)"},
    },
    "New Mexico": {
        "aliases": ["New Mexico(H)"],
        "sources": {"torvik": "New Mexico(H)"},
    },
}


def main():
    with open(CROSSWALK_PATH, "r") as f:
        data = json.load(f)

    existing = data["canonical"]
    existing_by_name = {e["name"]: e for e in existing}

    added = 0
    merged = 0
    skipped = 0

    # 1. Merge into existing entries
    for canon_name, updates in MERGE_INTO_EXISTING.items():
        if canon_name not in existing_by_name:
            print(f"WARNING: Expected to merge into '{canon_name}' but not found in crosswalk")
            continue
        entry = existing_by_name[canon_name]
        for alias in updates.get("aliases", []):
            if alias not in entry["aliases"]:
                entry["aliases"].append(alias)
        entry["sources"].update(updates.get("sources", {}))
        merged += 1

    # 2. Add new entries — skip if canonical name already exists
    for new_entry in NEW_ENTRIES:
        if new_entry["name"] in existing_by_name:
            print(f"SKIP (already exists): {new_entry['name']}")
            skipped += 1
            continue
        existing.append(new_entry)
        existing_by_name[new_entry["name"]] = new_entry
        added += 1

    data["canonical"] = sorted(existing, key=lambda e: e["name"])

    with open(CROSSWALK_PATH, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nCrosswalk updated: {CROSSWALK_PATH}")
    print(f"  Added:  {added} new teams")
    print(f"  Merged: {merged} existing entries")
    print(f"  Skipped (already existed): {skipped}")
    print(f"  Total canonical teams: {len(data['canonical'])}")


if __name__ == "__main__":
    main()
