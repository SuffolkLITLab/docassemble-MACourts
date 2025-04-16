from docassemble.base.functions import server
from docassemble.base.geocode import GoogleV3GeoCoder
from docassemble.base.core import DAObject, DAList
from docassemble.base.util import path_and_mimetype, Address, LatitudeLongitude, prevent_dependency_satisfaction
from docassemble.base.legal import Court
import io, json, re, os, time
import typing
from typing import Any, Callable, List, Mapping, Optional, Set, Union, Tuple
from docassemble.webapp.playground import PlaygroundSection
from collections.abc import Iterable
import copy

# Needed for Boston Municipal Court
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point

__all__= ['MACourt','MACourtList','combined_locations', 'get_year_from_docket_number']



def test_write() -> str:
    area = PlaygroundSection('sources').get_area()
    fpath = os.path.join(area.directory, "test" + '.json')
    jdata = "test"
    f = open(fpath, 'w')
    f.write(jdata)
    f.close()
    area.finalize()
    return fpath

def try_to_populate_county(address: Address, force:bool = False) -> None:
    """
    Jurisdiction depends on exactly matching names for county, city, etc. but we can't ask
    the user to say what county they live in as people do not know in Massachusetts.

    Try to fill in the `county` attribute of the specified address by geocoding the address.
    Sometimes Google's database is incomplete. If so, reverse geocode the latitude and longitude
    to try to get the county again. Finally, fall back to setting the `county` attribute to "unknown". """
    # Geocoding will fill in location attributes if not already defined
    if not hasattr(address, "location") or not hasattr(address.location, "latitude") or not address.location.latitude:
        try:
            address.geocode()
        except:
            pass
    # Don't reverse geocode if the address already has a county
    if not force and hasattr(address, "county"):
        return
    county = 'Unknown'
    try:
        geocoder = GoogleV3GeoCoder(server=server)
        geocoder.initialize()
    except:
        address.county = county
        return
    try:
        for item in geocoder.geocoder.reverse((address.location.latitude, address.location.longitude)).raw["address_components"]:
            if "administrative_area_level_2" in item["types"]:
                county = item["long_name"]
                break
    except:
        pass
    address.county = county   

class MACourt(Court):
    """Object representing a court in Massachusetts.
    TODO: it could be interesting to store a jurisdiction on a court. But this is non-trivial. Should it be geo boundaries?
    A list of cities? A list of counties? Instead, we use a function on the CourtList object that filters courts by
    address and can use any of those three features of the court to do the filtering."""
    def init(self, *pargs, **kwargs):
        super(MACourt, self).init(*pargs, **kwargs)
        if 'address' not in kwargs:
            self.initializeAttribute('address', Address)
        if 'jurisdiction' not in kwargs: # This attribute isn't used. Could be a better way to handle court locating
            self.jurisdiction = list()
        if 'location' not in kwargs:
            self.initializeAttribute('location', LatitudeLongitude)

    @property
    def phone_number(self):
      return getattr(self, 'phone')
    
    def __str__(self) -> str:
        return str(self.name)
      
    def _map_info(self) -> List[Mapping[str, Any]]:
        the_info = str(self.name)
        the_info += "  [NEWLINE]  " + self.address.block()
        result = {'latitude': self.location.latitude, 'longitude': self.location.longitude, 'info': the_info}
        if hasattr(self, 'icon'):
            result['icon'] = self.icon
        return [result]
      
    def short_label(self) -> str:
      """
      Returns a string that represents a nice, disambiguated label for the court.
      This may not match the court's name. If the name omits city, we
      append city name to the court name. This is good for a drop-down selection
      list.
      """
      if self.address.city in str(self.name):
        return str(self.name)
      else:
        return str(self.name) + ' (' + self.address.city + ')'
    
    def short_label_and_address(self) -> str:
      """
      Returns a markdown formatted string with the name and address of the court.
      More concise version without description; suitable for a responsive case.
      """
      return '**' + self.short_label() + '**' + '[BR]' + self.address.on_one_line()
    
    def short_description(self) -> str:
      """
      Returns a Markdown formatted string that includes the disambiguated name and 
      the description of the court, for inclusion in the results page with radio
      buttons.
      """
      return '**' + self.short_label() + '**' + '[BR]' + self.address.on_one_line() + '[BR]' + self.description
      

class MACourtList(DAList):
    """Represents a list of courts in Massachusetts. Package includes a cached list that is scraped from mass.gov"""
    def init(self, *pargs, **kwargs):
        super(MACourtList, self).init(*pargs, **kwargs)
        self.auto_gather = False
        self.gathered = True
        self.object_type = MACourt
        if not hasattr(self, 'data_path'):
            self.data_path = 'docassemble.MACourts:data/sources/' # use the system-wide installed version of the JSON files
        if hasattr(self,'courts'):
            if isinstance(self.courts, Iterable):
                self.load_courts(courts=self.courts,data_path=self.data_path)
            elif self.courts is True:
                self.load_courts(data_path=self.data_path)

    def filter_courts(self, court_types: Union[str, Iterable]) -> Optional[List]:
        """Return the list of courts matching the specified department(s). 
        E.g., Housing Court. court_types may be list or single court department."""
        if isinstance(court_types, str):
            return self.filter(department=court_types)
        elif isinstance(court_types, Iterable):
            return [court for court in self.elements if court.department in court_types]
        else:
            return None

    def get_court_by_code(self, court_code: str) -> Optional[MACourt]:
        """Return a court that has the matching court_code"""
        if isinstance(court_code, str):
            return next((court for court in self if str(court.court_code).lower() == court_code.lower()), None)
        else:
            return None

    def matching_courts(self, address: Union[Address, typing.Iterable[Address]], court_types: Optional[Union[str, typing.Iterable[str]]]=None) -> List[MACourt]:
        """Return a list of courts serving the specified address(es). Optionally limit to one or more types of courts"""
        if isinstance(address, Iterable):
            courts: Set[MACourt] = set()
            for add in address:
                try:
                    # It's helpful to normalize the address, but it's OK if 
                    # geolocation fails
                    # Populating county also geocodes
                    try_to_populate_county(add)
                except:
                    pass
                res = self.matching_courts_single_address(add, court_types)
                if isinstance(res, Iterable):
                    courts.update(filter(lambda el: el is not None, res))
                elif not res is None:
                    courts.add(res)
            return sorted(list(courts), key=lambda y: y.name)
        else:
            try_to_populate_county(address)
            return self.matching_courts_single_address(address, court_types)

    def matching_courts_single_address(self, address: Address, court_types: Optional[Union[str, typing.Iterable[str]]]=None) -> List[MACourt]:
        try:
          # Don't match Suffolk County in New York, e.g.
          if not address.state.lower() in ["ma","massachusetts"]:
            return []
        except:
          pass
        court_type_map: Mapping[str, Callable[[Address], Union[Set[MACourt], MACourt, None]]] = {
            'Housing Court': self.matching_housing_court,
            'District Court': self.matching_district_court,
            'Boston Municipal Court': self.matching_bmc,
            'Juvenile Court': self.matching_juvenile_court,
            'Land Court': self.matching_land_court,
            'Probate and Family Court': self.matching_probate_and_family_court,
            'Superior Court': self.matching_superior_court,
            'Appeals Court': self.matching_appeals_court,
        }
        if court_types is None:
            court_types = []

        if isinstance(court_types, str):
          if court_types in court_type_map:
            res = court_type_map[court_types](address)
            if isinstance(res, Iterable):
              return list(res)
            elif res is not None:
              return [res]
            else:
              return []
          else:
            return []
        elif isinstance(court_types, Iterable):
            matches: Set[MACourt] = set()
            for court_type in court_types:
              if court_type in court_type_map:
                res = court_type_map[court_type](address)
                if isinstance(res, Iterable):
                    matches.update(filter(lambda el: el is not None, res))
                elif not res is None:
                    matches.add(res)
              else:
                return []
            return sorted(list(matches), key=lambda y: y.name)
        else:
            raise Exception("NotAList")
        #     # Return all of the courts if court_types is not filtering the results
        #     matches = set()
        #     for court_type in court_type_map:
        #         res =  court_type_map[court_type](address)
        #         if isinstance(res, Iterable):
        #             matches.update(res)
        #         elif not res is None:
        #             matches.add(res)
        # if not len(matches):
        #     return None
        return list(matches)

    def load_courts(self, courts=['housing_courts','bmc','district_courts','superior_courts'], data_path:Optional[str]=None):
        """
        Load a set of courts into the MACourtList. Courts should be a list of names of JSON files in the data/sources directory.
        Will fall back on loading courts directly from MassGov if the cached file doesn't exist.
        Available courts:
        * district_courts,
        * housing_courts,
        * bmc,
        * superior_courts,
        * land_court,
        * juvenile_courts,
        * probate_and_family_courts,
        * appeals_court
        """
        if data_path is None:
          if hasattr(self, 'data_path'):
            data_path = self.data_path
          else:
            data_path = 'docassemble.MACourts:data/sources/'
        try:
            for court in courts:
                self.load_courts_from_file(court, data_path=data_path)
        except IOError:
            for court in courts:
              pass # no longer implemented
              # self.load_courts_from_massgov_by_filename(court)
        self.sort(key=lambda y: y.name)

    def load_courts_from_file(self, court_name, data_path='docassemble.MACourts:data/sources/'):
        """Add the list of courts at the specified JSON file into the current list"""

        json_path = court_name

        # 'housing_courts','bmc','district_courts','superior_courts'
        if court_name == 'housing_courts':
            court_department = 'Housing Court'
        elif court_name == 'bmc':
            court_department = 'Boston Municipal Court'
        elif court_name == 'district_courts':
            court_department = 'District Court'
        elif court_name == 'superior_courts':
            court_department = 'Superior Court'
        elif court_name == 'juvenile_courts':
            court_department = 'Juvenile Court'
        elif court_name in ['land_courts', 'land_court']:
            court_department = 'Land Court'
        elif court_name == 'probate_and_family_courts':
            court_department = 'Probate and Family Court'
        elif court_name == "appeals_court":
            court_department = "Appeals Court"

        path = path_and_mimetype(os.path.join(data_path, json_path+'.json'))[0]
        if path is None:
          # fallback, for running on non-docassemble.
          path = os.path.join(data_path, json_path + '.json')

        # Byte-order-marker is not allowed in JSON spec
        with open(path) as courts_json:
            courts = json.load(courts_json)

        for item in courts:
            # translate the dictionary data into an MACourtList
            court = self.appendObject()
            court.court_code = item.get('court_code')
            court.tyler_code = item.get('tyler_code')
            court.tyler_lower_court_code = item.get('tyler_lower_court_code')
            court.tyler_prod_lower_court_code = item.get('tyler_prod_lower_court_code')
            court.name = item['name']
            court.department = court_department
            court.division = parse_division_from_name(item['name'])
            court.phone = item['phone']
            court.fax = item['fax']
            court.location.latitude = item['location']['latitude']
            court.location.longitude = item['location']['longitude']
            court.has_po_box = item.get('has_po_box')
            court.description = item.get('description')
            court.address.address = item['address']['address']
            court.address.city = item['address']['city']
            court.address.state = item['address']['state']
            court.address.zip = item['address']['zip']
            court.address.county = item['address']['county']
            court.address.orig_address = item['address'].get('orig_address')
            court.ada_coordinators = item.get("ada_coordinators",[])

    def matching_juvenile_court(self, address) -> Set[MACourt]:
        """Returns either single matching MACourt object or a set of MACourts"""
        court_name = self.matching_juvenile_court_name(address)
        
        if isinstance(court_name,Iterable):
            # Many court names, one address
            courts = set()
            for court_item in court_name:
                courts.update(set([court for court in self.elements if court.name.rstrip().lower() == court_item.lower()]))
            return courts
        else: # this branch shouldn't be reached anymore -- we always return a set
            # one court name, which may match more than one court location. Sessions/sittings don't always get unique names
            return set([court for court in self.elements if court.name.rstrip().lower() == court_name.lower()])

    def matching_juvenile_court_name(self, address, depth=0) -> Set[str]:
        if depth == 1 and hasattr(address, 'norm_long') and hasattr(address.norm_long, 'city') and hasattr(address.norm_long, 'county'):
            address_to_compare = address.norm_long
        else:
            address_to_compare = address
        if (not hasattr(address_to_compare, 'county')) or (address_to_compare.county.lower().strip() == ''):
            if address_to_compare.city.lower() in ['boston', 'charlestown', 'dorchester','roxbury', 'jamaica plain', 'brighton', 'allston']:
                address_to_compare.county = "Suffolk County"
            else:
                return set()

        matches = []
        # Special case for two areas of Boston -- concurrent with BMC jurisdiction. Need to match these first
        if str(self.matching_bmc(address)) == "West Roxbury Division, Boston Municipal Court":
            return set(["West Roxbury Juvenile Court"])
        elif str(self.matching_bmc(address)) == "Dorchester Division, Boston Municipal Court":
            return set(["Dorchester Juvenile Court"])
        if address_to_compare.city.lower() in ["attleboro", "mansfield", "north attleboro","north attleborough", "norton"]:
            matches.append("Attleboro Juvenile Court")
        if address_to_compare.city.lower() in ["barnstable", "sandwich", "yarmouth"]:
            matches.append("Barnstable Juvenile Court")
        if address_to_compare.city.lower() in ["belchertown", "granby", "ware"]:
            matches.append("Belchertown Juvenile Court")
        if (address_to_compare.city.lower() in ["chelsea", "revere", "east boston", "winthrop"] or
            (hasattr(address_to_compare,'neighborhood') and ((address_to_compare.city.lower() == "boston") and
                address_to_compare.neighborhood.lower() in ["east boston","central square", "day square", "eagle hill", "maverick square", "orient heights","jeffries point"]))):
            matches.append("Chelsea Juvenile Court")
        if address_to_compare.city.lower() in ["brighton", "charlestown", "roxbury", "south boston", "boston"]:
            matches.append("Boston Juvenile Court")
        if address_to_compare.city.lower() in ["abington", "bridgewater", "brockton", "east bridgewater", "west bridgewater", "whitman"]:
            matches.append("Brockton Juvenile Court")
        if address_to_compare.city.lower() in ["arlington", "belmont", "cambridge", "everett", "malden", "medford", "melrose", "somerville","wakefield", "stoneham"]:
            matches.append("Cambridge Juvenile Court")
        if address_to_compare.city.lower() in ["avon", "canton", "dedham", "dover", "foxborough", "franklin", "medfield", "millis", "needham", "norfolk", "norwood", "plainville", "sharon", "stoughton", "walpole", "wellesley", "westwood", "wrentham","medway"]:
            matches.append("Dedham Juvenile Court")
        if address_to_compare.city.lower() in ["charlton", "dudley", "oxford", "southbridge", "sturbridge", "webster"]:
            matches.append("Dudley Juvenile Court")
        if address_to_compare.city.lower() in ["aquinnah", "chilmark", "edgartown", "gosnold", "oak bluffs", "tisbury", "west tisbury"]:
            matches.append("Edgartown Juvenile Court")
        if address_to_compare.city.lower() in ["fall river", "freetown", "somerset", "swansea", "westport"]:
            matches.append("Fall River Juvenile Court")
        if address_to_compare.city.lower() in ["bourne", "falmouth", "mashpee"]:
            matches.append("Falmouth Juvenile Court")
        if address_to_compare.city.lower() in ["ashburnham", "fitchburg", "gardner", "hubbardston", "lunenburg", "petersham", "phillipston", "princeton", "templeton", "westminster", "winchendon","royalston"]:
            matches.append("Fitchburg Juvenile Court")
        if address_to_compare.city.lower() in ["acton", "ashland", "bedford", "carlisle", "concord", "framingham", "holliston", "hudson", "lexington", "lincoln", "marlborough","marlboro", "maynard", "natick", "sherborn", "stow", "sudbury", "wayland","hopkinton"]:
            matches.append("Framingham Juvenile Court")
        if address_to_compare.city.lower() in ["alford", "becket", "egremont", "great barrington", "lee", "lenox", "monterey", "new marlborough", "otis", "sandisfield", "sheffield", "stockbridge", "tyringham", "west stockbridge"]:
            matches.append("Great Barrington Juvenile Court")
        if address_to_compare.city.lower() in ["ashfield", "bernardston", "buckland", "charlemont", "colrain", "conway", "deerfield", "gill", "greenfield", "hawley", "heath", "leyden", "monroe", "montague", "northfield", "rowe", "shelburne","shelburne falls", "sunderland", "whately"]:
            matches.append("Greenfield Juvenile Court")
        if address_to_compare.city.lower() in ["amherst", "chesterfield", "cummington", "easthampton", "goshen", "hadley", "hatfield", "middlefield", "northampton", "pelham", "plainfield", "southampton", "south hadley", "westhampton", "williamsburg", "worthington","huntington"]:
            matches.append("Hadley Juvenile Court")
        if address_to_compare.city.lower() in ["hanover", "hingham", "hull", "norwell", "rockland", "scituate"]:
            matches.append("Hingham Juvenile Court")
        if address_to_compare.city.lower() in ["blandford", "chester", "granville", "holyoke", "montgomery", "russell", "southwick", "westfield", "tolland"]:
            matches.append("Holyoke Juvenile Court")
        if address_to_compare.city.lower() in ["andover", "boxford", "bradford", "georgetown", "groveland", "haverhill", "lawrence", "north andover","methuen"]:
            matches.append("Lawrence Juvenile Court")
        if address_to_compare.city.lower() in ["ashby", "ayer", "billerica", "boxborough", "burlington", "chelmsford", "dracut", "dunstable", "groton", "littleton", "lowell", "north reading", "pepperell", "reading", "shirley", "tewksbury", "townsend", "tyngsborough", "westford", "wilmington", "winchester", "woburn"]:
            matches.append("Lowell Juvenile Court")
        if address_to_compare.city.lower() in ["lynn", "marblehead", "nahant", "saugus", "swampscott"]:
            matches.append("Lynn Juvenile Court")
        if address_to_compare.city.lower() in ["bellingham", "blackstone", "douglas", "hopedale", "mendon", "milford", "millville", "sutton", "upton", "uxbridge", "northbridge"]:
            matches.append("Milford Juvenile Court")
        if address_to_compare.county.lower() in ["nantucket county"]:
            matches.append("Nantucket Juvenile Court")
        if address_to_compare.city.lower() in ["acushnet", "dartmouth", "fairhaven", "freetown", "new bedford", "westport"]:
            matches.append("New Bedford Juvenile Court")
        if address_to_compare.city.lower() in ["amesbury", "essex", "hamilton", "ipswich", "merrimac", "newbury", "newburyport", "salisbury", "topsfield", "wenham", "west newbury", "gloucester","rockport","rowley"]:
            matches.append("Newburyport Juvenile Court")
        if address_to_compare.city.lower() in ["adams", "cheshire", "clarksburg", "florida", "hancock", "new ashford", "north adams", "williamstown", "windsor"]:
            matches.append("North Adams Juvenile Court")
        if address_to_compare.city.lower() in ["athol", "erving", "leverett", "new salem", "orange", "shutesbury", "warwick","wendell"]:
            matches.append("Orange Juvenile Court")
        if address_to_compare.city.lower() in ["brewster", "chatham", "dennis", "eastham", "harwich", "orleans", "provincetown", "wellfleet"]:
            matches.append("Orleans Juvenile Court")
        if address_to_compare.city.lower() in ["brimfield", "east longmeadow", "hampden", "holland", "ludlow", "monson", "palmer", "wales", "wilbraham"]:
            matches.append("Palmer Juvenile Court")
        if address_to_compare.city.lower() in ["becket", "dalton", "hancock", "hinsdale", "lanesborough", "lenox", "peru", "pittsfield", "richmond", "washington", "windsor"]:
            matches.append("Pittsfield Juvenile Court")
        if address_to_compare.city.lower() in ["duxbury", "halifax", "hanson", "kingston", "marshfield", "pembroke", "plymouth", "plympton"]:
            matches.append("Plymouth Juvenile Court")
        if address_to_compare.city.lower() in ["braintree", "cohasset", "holbrook", "milton", "quincy", "randolph", "weymouth"]:
            matches.append("Quincy Juvenile Court")
        if address_to_compare.city.lower() in ["beverly", "danvers", "manchester by the sea", "manchester-by-the-sea", "middleton", "salem","lynnfield", "peabody"]:
            matches.append("Salem Juvenile Court")
        if address_to_compare.city.lower() in ["agawam", "chicopee", "longmeadow", "springfield", "west springfield"]:
            matches.append("Springfield Juvenile Court")
        # if address_to_compare.city.lower() in ["error"]: # Doesn't seem like this court has any regular cases
        #     matches.append("Stoughton Juvenile Court")
        if address_to_compare.city.lower() in ["berkley", "dighton", "easton", "raynham", "rehoboth", "seekonk", "taunton"]:
            matches.append("Taunton Juvenile Court")
        if address_to_compare.city.lower() in ["concord", "newton", "watertown", "waltham", "weston"]:
            matches.append("Waltham Juvenile Court")
        if address_to_compare.city.lower() in ["carver", "lakeville", "marion", "mattapoisett", "middleborough", "rochester", "wareham"]:
            matches.append("Wareham Juvenile Court")
        if address_to_compare.city.lower() in ["auburn", "barre", "berlin", "bolton", "boylston", "brookfield", "clinton", "east brookfield", "grafton", "hardwick", "harvard", "holden", "lancaster", "leicester", "millbury", "new braintree", "northborough", "north brookfield", "oakham", "princeton","paxton", "rutland", "shrewsbury", "southborough", "spencer", "sterling", "warren", "westborough", "west boylston", "west brookfield", "worcester","leominster"]:
            matches.append("Worcester Juvenile Court")
        if not matches and depth==0:
            return self.matching_juvenile_court_name(address, depth=1)
        return set(matches)

    def matching_probate_and_family_court(self, address) -> Set[MACourt]:
        """Returns either single matching MACourt object or a set of MACourts"""
        court_names = self.matching_probate_and_family_court_name(address)
        courts = set()
        for court_item in court_names:
            courts.update([court for court in self.elements if court.name.rstrip().lower() == court_item.lower()])
        return courts

    def matching_probate_and_family_court_name(self, address, depth=0) -> Set[str]:
        """Multiple P&F courts may serve the same address"""
        if depth==1 and hasattr(address, 'norm_long') and hasattr(address.norm_long, 'city') and hasattr(address.norm_long, 'county'):
            address_to_compare = address.norm_long
        else:
            address_to_compare = address
        if (not hasattr(address_to_compare, 'county')) or (address_to_compare.county.lower().strip() == ''):
            if address_to_compare.city.lower() in ['boston', 'charlestown', 'dorchester','roxbury', 'jamaica plain', 'brighton', 'allston']:
                address_to_compare.county = "Suffolk County"
            else:
                return set()
        matches = []

        if (address_to_compare.county.lower() == "barnstable county") or (address_to_compare.city.lower() in ["bourne", "brewster", "chatham", "dennis", "eastham", "falmouth", "harwich", "mashpee", "orleans", "provincetown", "sandwich", "truro", "wellfleet", "yarmouth"]):
            matches.append("Barnstable Probate and Family Court")
        if (address_to_compare.county.lower() == "berkshire county") or (address_to_compare.city.lower() in ["adams", "alford", "becket", "cheshire", "clarksburg", "dalton", "egremont", "florida", "great barrington", "hancock", "hinsdale", "lanesborough", "lee", "lenox", "monterey", "mount washington", "new ashford", "new marlborough", "north adams", "otis", "peru", "pittsfield", "richmond", "sandisfield", "savoy", "sheffield", "stockbridge", "tyringham", "washington", "west stockbridge", "williamstown", "windsor"]):
            matches.append("Berkshire Probate and Family Court")
        if (address_to_compare.county.lower() == "bristol county") or (address_to_compare.city.lower() in ["acushnet", "attleboro", "berkley", "dartmouth", "dighton", "easton", "fairhaven", "fall river", "freetown", "mansfield", "new bedford", "north attleborough", "norton", "raynham", "rehoboth", "seekonk", "somerset", "swansea", "taunton", "westport"]):
            matches.extend(["Bristol Probate and Family Court","Fall River Probate and Family Court","New Bedford Probate and Family Court"])
        if (address_to_compare.county.lower() == "dukes county") or (address_to_compare.city.lower() in ["aquinnah", "chilmark", "edgartown", "gosnold", "oak bluffs", "tisbury", "west tisbury"]):
            matches.append("Dukes Probate and Family Court")
        if (address_to_compare.county.lower() == "essex county") or (address_to_compare.city.lower() in ["amesbury", "andover", "beverly", "boxford", "danvers", "essex", "georgetown", "gloucester", "groveland", "hamilton", "haverhill", "ipswich", "lawrence", "lynn", "lynnfield", "manchester by the sea", "manchester-by-the-sea", "marblehead", "merrimac", "methuen", "middleton", "nahunt", "newbury", "newburyport", "north andover", "peabody", "rockport", "rowley", "salem", "salisbury", "saugus", "swampscott", "topsfield", "wenham", "west newbury"]):
            matches.extend(["Essex Probate and Family Court", "Lawrence Probate and Family Court"])
        if (address_to_compare.county.lower() == "franklin county") or (address_to_compare.city.lower() in ["ashfield", "bernardston", "buckland", "charlemont", "colrain", "conway", "deerfield", "erving", "gill", "greenfield", "hawley", "heath", "leverett", "leyden", "monroe", "montague", "new salem", "northfield", "orange", "rowe", "shelburne","shelburne falls", "shutesbury", "sunderland", "warwick", "wendell", "whately"]):
            matches.append("Franklin Probate and Family Court")
        if (address_to_compare.county.lower() == "hampden county") or (address_to_compare.city.lower() in ["agawam", "blandford", "brimfield", "chester", "chicopee", "east longmeadow", "granville", "hampden", "holland", "holyoke", "longmeadow", "ludlow", "monson", "montgomery", "palmer", "russell", "southwick", "springfield", "tolland", "wales", "west springfield", "westfield", "wilbraham"]):
            matches.append("Hampden Probate and Family Court")
        if (address_to_compare.county.lower() == "hampshire county") or (address_to_compare.city.lower() in ["amherst", "belchertown", "chesterfield", "cummington", "easthampton", "goshen", "granby", "hadley", "hatfield", "huntington", "middlefield", "northampton", "pelham", "plainfield", "south hadley", "southamptom", "ware", "westhampton", "williamsburg", "worthington"]):
            matches.append("Hampshire Probate and Family Court")
        if (address_to_compare.county.lower() == "middlesex county") and (address_to_compare.city.lower() in ["arlington","belmont","cambridge","everett","lexington","malden","medford","melrose","newton","somerville","stoneham","wakefield","waltham","watertown","weston","winchester","woburn"]):
            matches.append("Middlesex Probate and Family Court - South")
        if (address_to_compare.county.lower() == "middlesex county") and (address_to_compare.city.lower() in ["acton","ashby","ashland","ayer","bedford","billerica","boxborough","burlington","carlisle","chelmsford","concord","dracut","dunstable","framingham","groton","holliston","hopkinton","hudson","lincoln","littleton","lowell","marlborough","maynard","natick","northreading","pepperrell","reading","sherborn","shirley","stow","sudbury","tewksbury","townsend","tyngsborough","wayland","westford","wilmington"]):
            matches.append("Middlesex Probate and Family Court - North")	
        if (address_to_compare.county.lower() == "nantucket county") or (address_to_compare.city.lower() in ["nantucket"]):
            matches.append("Nantucket Probate and Family Court")
        if (address_to_compare.county.lower() == "norfolk county") or (address_to_compare.city.lower() in ["avon", "bellingham", "braintree", "brookline", "canton", "cohasset", "dedham", "dover", "foxborough", "franklin", "holbrook", "medfield", "medway", "millis", "milton", "needham", "norfolk", "norwood", "plainville", "quincy", "randolph", "sharon", "stoughton", "walpole", "wellesley", "westwood", "weymouth", "wrentham"]):
            matches.append("Norfolk Probate and Family Court")
        if (address_to_compare.county.lower() == "plymouth county") or (address_to_compare.city.lower() in ["abington", "bridgewater", "brockton", "carver", "duxbury", "east bridgewater", "halifax", "hanover", "hanson", "hingham", "hull", "kingston", "lakeville", "marion", "marshfield", "mattapoisett", "middleborough", "norwell", "pembroke", "plymouth", "rochester", "rockland", "scituate", "wareham", "west bridgewater", "whitman"]):
            matches.append("Plymouth Probate and Family Court")
        if (address_to_compare.county.lower() == "suffolk county") or (address_to_compare.city.lower() in ["boston", "chelsea", "revere", "winthrop"]):
            matches.append("Suffolk Probate and Family Court")
        if (address_to_compare.county.lower() == "worcester county") or (address_to_compare.city.lower() in ["ashburnham", "athol", "auburn", "barre", "berlin", "blackstone", "bolton", "boylston", "brookfield", "charlton", "clinton", "douglas", "dudley", "east brookfield", "fitchburg", "gardner", "grafton", "hardwick", "harvard", "holden", "hopedale", "hubbardston", "lancaster", "leicester", "leominster", "lunenburg", "mendon", "milford", "millbury", "millville", "new braintree", "north brookfield", "northborough", "northbridge", "oakham", "oxford", "paxton", "petersham", "phillipston", "princeton", "royalston", "rutland", "shrewsbury", "southborough", "southbridge", "spencer", "sterling", "sturbridge", "sutton", "templeton", "upton", "uxbridge", "warren", "webster", "west boylston", "west brookfield", "westborough", "westminster", "winchendon", "worcester"]):
            matches.append("Worcester Probate and Family Court")
        if address_to_compare.city.lower() in ["abington", "bridgewater", "brockton", "carver", "duxbury", "east bridgewater", "halifax", "hanover", "hanson", "hingham", "hull", "kingston", "lakeville", "marion", "marshfield", "mattapoisett", "middleboro", "norwell", "pembroke" , "plymouth", "plympton", "rochester", "rockland", "scituate", "wareham", "west bridgewater", "whitman"]:
            matches.append("Brockton Probate and Family Court")

        if not matches and depth==0:
            return self.matching_probate_and_family_court_name(address, depth=1)
        return set(matches)

    def matching_superior_court(self, address: Address) -> Set[MACourt]:
        """Returns either single matching MACourt object or a set of MACourts"""
        court_name = self.matching_superior_court_name(address)
        # if isinstance(court_name,Iterable):
        #     courts = set()
        #     for court_item in court_name:
        #         courts.update(set([court for court in self.elements if court.name.rstrip().lower() == court_item.lower()]))
        #     return courts
        # else:
        return set([court for court in self.elements if court.name.rstrip().lower() == court_name.lower()])
        # return next ((court for court in self.elements if court.name.rstrip().lower() == court_name.lower()), None)

    def matching_superior_court_name(self, address: Address, depth=0) -> str:
        if depth == 1 and hasattr(address, 'norm_long') and hasattr(address.norm_long, 'city') and hasattr(address.norm_long, 'county'):
            address_to_compare = address.norm_long
        else:
            address_to_compare = address
        if (not hasattr(address_to_compare, 'county')) or (address_to_compare.county.lower().strip() == ''):
            if address_to_compare.city.lower() in ['boston', 'charlestown', 'dorchester','roxbury', 'jamaica plain', 'brighton', 'allston']:
                address_to_compare.county = "Suffolk County"
            else:
                return ''
        if (address_to_compare.county.lower() == "barnstable county") or (address_to_compare.city.lower() in ["barnstable", "bourne", "brewster", "chatham", "dennis", "eastham", "falmouth", "harwich", "mashpee", "orleans", "provincetown", "sandwich", "truro", "wellfleet", "yarmouth"]):
                local_superior_court = "Barnstable County Superior Court"
        elif (address_to_compare.county.lower() == "berkshire county") or (address_to_compare.city.lower() in ["adams", "alford", "becket", "cheshire", "clarksburg", "dalton", "egremont", "florida", "great barrington", "hancock", "hinsdale", "lanesborough", "lee", "lenox", "monterey", "mount washington", "new ashford", "new marlborough", "north adams", "otis", "peru", "pittsfield", "richmond", "sandisfield", "savoy", "sheffield", "stockbridge", "tyringham", "washington", "west stockbridge", "williamstown", "windsor"]):
                local_superior_court = "Berkshire County Superior Court"
        elif (address_to_compare.county.lower() == "bristol county") or (address_to_compare.city.lower() in ["acushnet", "attleboro", "berkley", "dartmouth", "dighton", "easton", "fairhaven", "fall river", "freetown", "mansfield", "new bedford", "north attleborough", "norton", "raynham", "rehoboth", "seekonk", "somerset", "swansea", "taunton", "westport"]):
                local_superior_court = "Bristol County Superior Court"
        elif (address_to_compare.county.lower() == "dukes county") or (address_to_compare.city.lower() in ["aquinnah", "chilmark", "edgartown", "gosnold", "oak bluffs", "tisbury", "west tisbury"]):
                local_superior_court = "Dukes County Superior Court"
        elif (address_to_compare.county.lower() == "essex county") or (address_to_compare.city.lower() in ["amesbury", "andover", "beverly", "boxford", "danvers", "essex", "georgetown", "gloucester", "groveland", "hamilton", "haverhill", "ipswich", "lawrence", "lynn", "lynnfield", "manchester by the sea", "manchester-by-the-sea", "marblehead", "merrimac", "methuen", "middleton", "nahunt", "newbury", "newburyport", "north andover", "peabody", "rockport", "rowley", "salem", "salisbury", "saugus", "swampscott", "topsfield", "wenham", "west newbury"]):
                local_superior_court = "Essex County Superior Court"
                #local_superior_court = ["Essex County Superior Court", "Essex County Superior Court - Lawrence", "Essex County Superior Court - Newburyport"]
        elif (address_to_compare.county.lower() == "franklin county") or (address_to_compare.city.lower() in ["ashfield", "bernardston", "buckland", "charlemont", "colrain", "conway", "deerfield", "erving", "gill", "greenfield", "hawley", "heath", "leverett", "leyden", "monroe", "montague", "new salem", "northfield", "orange", "rowe", "shelburne","shelburne falls", "shutesbury", "sunderland", "warwick", "wendell", "whately"]):
                local_superior_court = "Franklin County Superior Court"
        elif (address_to_compare.county.lower() == "hampden county") or (address_to_compare.city.lower() in ["agawam", "blandford", "brimfield", "chester", "chicopee", "east longmeadow", "granville", "hampden", "holland", "holyoke", "longmeadow", "ludlow", "monson", "montgomery", "palmer", "russell", "southwick", "springfield", "tolland", "wales", "west springfield", "westfield", "wilbraham"]):
                local_superior_court = "Hampden County Superior Court"
        elif (address_to_compare.county.lower() == "hampshire county") or (address_to_compare.city.lower() in ["amherst", "belchertown", "chesterfield", "cummington", "easthampton", "goshen", "granby", "hadley", "hatfield", "huntington", "middlefield", "northampton", "pelham", "plainfield", "south hadley", "southamptom", "ware", "westhampton", "williamsburg", "worthington"]):
                local_superior_court = "Hampshire County Superior Court"
        elif (address_to_compare.county.lower() == "middlesex county") or (address_to_compare.city.lower() in ["acton", "arlington", "ashby", "ashland", "ayer", "bedford", "belmont", "billerica", "boxborough", "burlington", "cambridge", "carlisle", "chelmsford", "concord", "dracut", "dunstable", "everett", "framingham", "groton", "holliston", "hopkinton", "hudson", "lexington", "lincoln", "littleton", "lowell", "malden", "marlborough","marlboro", "maynard", "medford", "melrose", "natick", "newton", "north reading", "pepperell", "reading", "sherborn", "shirley", "somerville", "stoneham", "stow", "sudbury", "tewksbury", "townsend", "tyngsborough", "wakefield", "waltham", "watertown", "wayland", "westford", "weston", "wilmington", "winchester", "woburn"]):
                local_superior_court = "Middlesex County Superior Court"
                #local_superior_court = ["Middlesex County Superior Court", "Middlesex County Superior Court - Lowell"]
        elif (address_to_compare.county.lower() == "nantucket county") or (address_to_compare.city.lower() in ["nantucket"]):
                local_superior_court = "Nantucket County Superior Court"
        elif (address_to_compare.county.lower() == "norfolk county") or (address_to_compare.city.lower() in ["avon", "bellingham", "braintree", "brookline", "canton", "cohasset", "dedham", "dover", "foxborough", "franklin", "holbrook", "medfield", "medway", "millis", "milton", "needham", "norfolk", "norwood", "plainville", "quincy", "randolph", "sharon", "stoughton", "walpole", "wellesley", "westwood", "weymouth", "wrentham"]):
                local_superior_court = "Norfolk County Superior Court"
        elif (address_to_compare.county.lower() == "plymouth county") or (address_to_compare.city.lower() in ["abington", "bridgewater", "brockton", "carver", "duxbury", "east bridgewater", "halifax", "hanover", "hanson", "hingham", "hull", "kingston", "lakeville", "marion", "marshfield", "mattapoisett", "middleborough", "norwell", "pembroke", "plymouth", "rochester", "rockland", "scituate", "wareham", "west bridgewater", "whitman"]):
                local_superior_court = "Plymouth County Superior Court"
        elif (address_to_compare.county.lower() == "suffolk county") or (address_to_compare.city.lower() in ["boston", "chelsea", "revere", "winthrop"]):
                local_superior_court = "Suffolk County Superior Court"
        elif (address_to_compare.county.lower() == "worcester county") or (address_to_compare.city.lower() in ["ashburnham", "athol", "auburn", "barre", "berlin", "blackstone", "bolton", "boylston", "brookfield", "charlton", "clinton", "douglas", "dudley", "east brookfield", "fitchburg", "gardner", "grafton", "hardwick", "harvard", "holden", "hopedale", "hubbardston", "lancaster", "leicester", "leominster", "lunenburg", "mendon", "milford", "millbury", "millville", "new braintree", "north brookfield", "northborough", "northbridge", "oakham", "oxford", "paxton", "petersham", "phillipston", "princeton", "royalston", "rutland", "shrewsbury", "southborough", "southbridge", "spencer", "sterling", "sturbridge", "sutton", "templeton", "upton", "uxbridge", "warren", "webster", "west boylston", "west brookfield", "westborough", "westminster", "winchendon", "worcester"]):
                local_superior_court = "Worcester County Superior Court"
        else:
            local_superior_court = ''
        if not local_superior_court and depth==0:
            return self.matching_superior_court_name(address, depth=1)
        return local_superior_court

    def matching_land_court(self, address: Address) -> Optional[MACourt]:
        """There's currently only one Land Court"""
        return next((court for court in self.elements if court.name.rstrip().lower() == 'land court'),None)
      
    def matching_appeals_court(self, address: Address) -> Optional[MACourt]:
        """Two appeals courts: single justice and panel. Returns the single justice one by default."""
        return next((court for court in self.elements if court.name.rstrip().lower() == 'massachusetts appeals court (single justice)'),None)
      
    def matching_district_court(self, address: Address) -> Set[MACourt]:
        """Return list of MACourts representing the District Court(s) serving the given address"""
        court_name = self.matching_district_court_name(address)
        courts = set()
        for court_item in court_name:
            matching_obj = next((court for court in self.elements if court.name.rstrip().lower() == court_item.lower()), None)
            if matching_obj:
                courts.add(matching_obj)
        return courts

    def matching_district_court_name(self, address: Address, depth=0) -> Set[str]:
        """Returns the name of the MACourt(s) representing the district court that covers the specified address.
        Harcoded and must be updated if court jurisdictions or names change. Address must specify county attribute
        
        At least one district court has overlapping jurisdiction: Northern Berkshire District Court  and Pittsfield District Court
        both serve two cities. This method will return both courts in a list.
        """
        if depth == 1 and hasattr(address, 'norm_long') and hasattr(address.norm_long, 'city') and hasattr(address.norm_long, 'county'):
            address_to_compare = address.norm_long
        else:
            address_to_compare = address
        if (not hasattr(address_to_compare, 'county')) or (address_to_compare.county.lower().strip() == ''):
            if address_to_compare.city.lower() in ['boston', 'charlestown', 'dorchester','roxbury', 'jamaica plain', 'brighton', 'allston']:
                address_to_compare.county = "Suffolk County"
            else:
                return set()
        matches = []
        if (address_to_compare.county.lower() == "dukes county") or (address_to_compare.city.lower() in ["edgartown", "oak bluffs", "tisbury", "west tisbury", "chilmark", "aquinnah", "gosnold", "elizabeth islands"]):
            matches.append("Edgartown District Court")
        if (address_to_compare.county.lower() == "nantucket county") or (address_to_compare.city.lower() in ["nantucket"]):
            matches.append( "Nantucket District Court")
        if address_to_compare.city.lower() in ["barnstable", "yarmouth", "sandwich"]:
            matches.append( "Barnstable District Court")
        if address_to_compare.city.lower() in ["attleboro", "mansfield", "north attleboro","north attleborough","norton"]:
            matches.append("Attleboro District Court")
        if address_to_compare.city.lower() in ["ashby", "ayer", "boxborough", "dunstable", "groton", "littleton", "pepperell", "shirley", "townsend", "westford", "devens regional enterprise zone"]:
            matches.append("Ayer District Court")
        if address_to_compare.city.lower() in ["abington", "bridgewater", "brockton", "east bridgewater", "west bridgewater", "whitman"]:
            matches.append("Brockton District Court")
        if address_to_compare.city.lower() in ["brookline"]:
            matches.append("Brookline District Court")
        if address_to_compare.city.lower() in ["cambridge", "arlington", "belmont"]:
            matches.append("Cambridge District Court")
        if address_to_compare.city.lower() in ["chelsea", "revere"]:
            matches.append("Chelsea District Court")
        if address_to_compare.city.lower() in ["chicopee"]:
            matches.append("Chicopee District Court")
        if address_to_compare.city.lower() in ["berlin", "bolton", "boylston", "clinton", "harvard", "lancaster", "sterling", "west boylston"]:
            matches.append("Clinton District Court")
        if address_to_compare.city.lower() in ["concord", "carlisle", "lincoln", "lexington", "bedford", "acton", "maynard", "stow"]:
            matches.append("Concord District Court")
        if address_to_compare.city.lower() in ["dedham", "dover", "medfield", "needham", "norwood", "wellesley", "westwood"]:
            matches.append("Dedham District Court")
        if address_to_compare.city.lower() in ["charlton", "dudley", "oxford", "southbridge", "sturbridge", "webster"]:
            matches.append("Dudley District Court")
        if address_to_compare.city.lower() in ["barre", "brookfield", "east brookfield", "hardwick", "leicester", "new braintree", "north brookfield", "oakham", "paxton", "rutland", "spencer", "warren", "west brookfield"]:
            matches.append("East Brookfield District Court")
        if address_to_compare.city.lower() in ["amherst", "belchertown", "granby", "hadley", "pelham", "south hadley", "ware", "m.d.c. quabbin reservoir", "watershed area"]:
            matches.append("Eastern Hampshire District Court")
        if address_to_compare.city.lower() in ["fall river", "freetown", "somerset", "swansea", "westport"]:
            matches.append("Fall River District Court")
        if address_to_compare.city.lower() in ["bourne", "falmouth", "mashpee"]:
            matches.append("Falmouth District Court")
        if address_to_compare.city.lower() in ["fitchburg", "lunenburg"]:
            matches.append("Fitchburg District Court")
        if address_to_compare.city.lower() in ["ashland", "framingham", "holliston", "hopkinton", "sudbury", "wayland"]:
            matches.append("Framingham District Court")
        if address_to_compare.city.lower() in ["gardner", "hubbardston", "petersham", "westminster"]:
            matches.append("Gardner District Court")
        if address_to_compare.city.lower() in ["essex", "gloucester", "rockport"]:
            matches.append("Gloucester District Court")
        if address_to_compare.city.lower() in ["ashfield", "bernardston", "buckland", "charlemont", "colrain", "conway", "deerfield", "gill", "greenfield", "hawley", "heath", "leyden", "monroe", "montague", "northfield", "rowe", "shelburne","shelburne falls", "sunderland", "whately"]:
            matches.append("Greenfield District Court")
        if address_to_compare.city.lower() in ["boxford", "bradford", "georgetown", "groveland", "haverhill"]:
            matches.append("Haverhill District Court")
        if address_to_compare.city.lower() in ["hanover", "hingham", "hull", "norwell", "rockland", "scituate"]:
            matches.append("Hingham District Court")
        if address_to_compare.city.lower() in ["holyoke"]:
            matches.append("Holyoke District Court")
        if address_to_compare.city.lower() in ["ipswich", "hamilton", "wenham", "topsfield"]:
            matches.append("Ipswich District Court")
        if address_to_compare.city.lower() in ["andover", "lawrence", "methuen", "north andover"]:
            matches.append("Lawrence District Court")
        if address_to_compare.city.lower() in ["holden", "princeton", "leominster"]:
            matches.append("Leominster District Court")
        if address_to_compare.city.lower() in ["billerica", "chelmsford", "dracut", "lowell", "tewksbury", "tyngsboro", "tyngsborough"]:
            matches.append("Lowell District Court")
        if address_to_compare.city.lower() in ["lynn", "marblehead", "nahant", "saugus", "swampscott"]:
            matches.append("Lynn District Court")
        if address_to_compare.city.lower() in ["malden", "melrose", "everett", "wakefield"]:
            matches.append("Malden District Court")
        if address_to_compare.city.lower() in ["marlborough","marlboro", "hudson"]:
            matches.append("Marlborough District Court")
        if address_to_compare.city.lower() in ["mendon", "upton", "hopedale", "milford", "bellingham"]:
            matches.append("Milford District Court")
        if address_to_compare.city.lower() in ["natick","sherborn"]:
            matches.append("Natick District Court")
        if address_to_compare.city.lower() in ["acushnet", "dartmouth", "fairhaven", "freetown", "new bedford", "westport"]:
            matches.append("New Bedford District Court")
        if address_to_compare.city.lower() in ["amesbury", "merrimac", "newbury", "newburyport", "rowley", "salisbury", "west newbury"]:
            matches.append("Newburyport District Court")
        if address_to_compare.city.lower() in ["newton"]:
            matches.append("Newton District Court")
        if address_to_compare.city.lower() in ["chesterfield", "cummington", "easthampton", "goshen", "hatfield", "huntington", "middlefield", "northampton", "plainfield", "southampton", "westhampton", "williamsburg", "worthington"]:
            matches.append("Northampton District Court")
        if address_to_compare.city.lower() in ["adams", "cheshire", "clarksburg", "florida", "hancock", "new ashford", "north adams", "savoy", "williamstown", "windsor"]:
            matches.append("Northern Berkshire District Court")
        if address_to_compare.city.lower() in ["athol", "erving", "leverett", "new salem", "orange", "shutesbury", "warwick", "wendell"]:
            matches.append("Orange District Court")
        if address_to_compare.city.lower() in ["brewster", "chatham", "dennis", "eastham", "orleans", "harwich", "truro", "wellfleet", "provincetown"]:
            matches.append("Orleans District Court")
        if address_to_compare.city.lower() in ["brimfield", "east longmeadow", "hampden", "holland", "ludlow", "monson", "palmer", "wales", "wilbraham"]:
            matches.append("Palmer District Court")
        if address_to_compare.city.lower() in ["lynnfield", "peabody"]:
            matches.append("Peabody District Court")
        if address_to_compare.city.lower() in ["becket", "dalton", "hancock", "hinsdale", "lanesborough", "lenox", "peru", "pittsfield", "richmond", "washington", "windsor"]:
            matches.append("Pittsfield District Court")
        if address_to_compare.city.lower() in ["duxbury", "halifax", "hanson", "kingston", "marshfield", "pembroke", "plymouth", "plympton"]:
            matches.append("Plymouth District Court")
        if address_to_compare.city.lower() in ["braintree", "cohasset", "holbrook", "milton", "quincy", "randolph", "weymouth"]:
            matches.append("Quincy District Court")
        if address_to_compare.city.lower() in ["beverly", "danvers", "manchester by the sea", "manchester-by-the-sea", "middleton", "salem"]:
            matches.append("Salem District Court")
        if address_to_compare.city.lower() in ["medford", "somerville"]:
            matches.append("Somerville District Court")
        if address_to_compare.city.lower() in ["alford", "becket", "egremont", "great barrington", "lee", "lenox", "monterey", "mount washington", "new marlborough", "otis", "sandisfield", "sheffield", "stockbridge", "tyringham", "west stockbridge"]:
            matches.append("Southern Berkshire District Court")
        if address_to_compare.city.lower() in ["longmeadow", "springfield", "west springfield"]:
            matches.append("Springfield District Court")
        if address_to_compare.city.lower() in ["avon", "canton", "sharon", "stoughton"]:
            matches.append("Stoughton District Court")
        if address_to_compare.city.lower() in ["berkley", "dighton", "easton", "raynham", "rehoboth", "seekonk", "taunton"]:
            matches.append("Taunton District Court")
        if address_to_compare.city.lower() in ["blackstone", "douglas", "millville", "northbridge", "sutton", "uxbridge"]:
            matches.append("Uxbridge District Court")
        if address_to_compare.city.lower() in ["waltham", "watertown", "weston"]:
            matches.append("Waltham District Court")
        if address_to_compare.city.lower() in ["carver", "lakeville", "mattapoisett", "middleboro", "middleborough", "rochester", "wareham","marion"]:
            matches.append("Wareham District Court")
        if address_to_compare.city.lower() in ["grafton", "northborough", "shrewsbury", "southborough", "westborough","westboro"]:
            matches.append("Westborough District Court")
        if address_to_compare.city.lower() in ["agawam", "blandford", "chester", "granville", "montgomery", "russell", "southwick", "tolland", "westfield"]:
            matches.append("Westfield District Court")
        if address_to_compare.city.lower() in ["ashburnham", "phillipston", "royalston", "templeton", "winchendon"]:
            matches.append("Winchendon District Court")
        if address_to_compare.city.lower() in ["burlington", "north reading", "reading", "stoneham", "wilmington", "winchester", "woburn"]:
            matches.append("Woburn District Court")
        if address_to_compare.city.lower() in ["auburn", "millbury", "worcester"]:
            matches.append("Worcester District Court")
        if address_to_compare.city.lower() in ["foxborough", "franklin", "medway", "millis", "norfolk", "plainville", "walpole", "wrentham"]:
            matches.append("Wrentham District Court")
        if not matches and depth == 0:
            return self.matching_district_court_name(address, depth=1)
        return set(matches)

    def matching_housing_court(self, address: Address) -> Optional[MACourt]:
        """Return the MACourt representing the Housing Court serving the given address"""
        court_name = self.matching_housing_court_name(address)
        return next ((court for court in self.elements if court.name.rstrip().lower() == court_name.lower()), None)

    def matching_housing_court_name(self, address: Address, depth=0) -> str:
        """Returns the name of the MACourt representing the housing court that covers the specified address.
        Harcoded and must be updated if court jurisdictions or names change. Address must specify county attribute"""

        #f hasattr(address, 'norm_long') and hasattr(address.norm_long, 'city') and hasattr(address.norm_long, 'county'):
        #    address_to_compare = address.norm_long
        #else:
        #    address_to_compare = address
        address_to_compare = address # don't normalize -- this screws up some addresses in small towns
        if (not hasattr(address_to_compare, 'county')) or (address_to_compare.county.lower().strip() == ''):
            if address_to_compare.city.lower() in ['boston', 'charlestown', 'dorchester','roxbury', 'jamaica plain', 'brighton', 'allston']:
                address_to_compare.county = "Suffolk County"
            else:
                return ''
        if ((address_to_compare.city.lower()) in ['charlestown','chelsea','revere','winthrop', 'east boston','e. boston'] or
            (hasattr(address_to_compare,'neighborhood') and ((address_to_compare.city.lower() == "boston") and
                address_to_compare.neighborhood.lower() in ["east boston","central square", "day square", "eagle hill", "maverick square", "orient heights","jeffries point"]))):
            local_housing_court = "Eastern Housing Court - Chelsea Session"
        elif (address_to_compare.county.lower() == "suffolk county") or (address_to_compare.city.lower() in ["brookline"]):
            local_housing_court = "Eastern Housing Court"
        elif address_to_compare.city.lower() in ["arlington","belmont","cambridge","medford","newton","somerville"]:
            local_housing_court = "Eastern Housing Court - Middlesex Session"
        elif address_to_compare.city.lower() in ["ashfield", "bernardston", "buckland", "charlemont", "colrain", "conway", "deerfield", "erving", "gill", "greenfield", "hawley", "heath", "leverett", "leyden", "monroe", "montague", "new salem", "northfield", "orange", "rowe", "shelburne","shelburne falls", "shutesbury", "sunderland", "warwick", "wendell", "whately"]:
            local_housing_court = "Western Housing Court - Greenfield Session"
        elif address_to_compare.city.lower() in ['amherst', 'belchertown', 'chesterfield', 'cummington', 'easthampton', 'goshen', 'granby', 'hadley', 'hatfield', 'huntington', 'middlefield', 'northampton', 'pelham', 'plainfield', 'south hadley', 'southampton', 'ware', 'westhampton', 'williamsburg','worthington']:
            local_housing_court = "Western Housing Court - Hadley Session"
        elif address_to_compare.county.lower() == "berkshire county":
            local_housing_court = "Western Housing Court - Pittsfield Session"
        elif address_to_compare.city.lower() in [
                                "tyringham",
                                "richmond",
                                "alford",
                                "otis",
                                "mount washington",
                                "williamstown",
                                "egremont",
                                "becket",
                                "sheffield",
                                "new ashford",
                                "west stockbridge",
                                "great barrington",
                                "cheshire",
                                "lenox",
                                "windsor",
                                "lanesborough",
                                "washington",
                                "new marlborough",
                                "monterey",
                                "stockbridge",
                                "sandisfield",
                                "hinsdale",
                                "hancock",
                                "county",
                                "lee",
                                "peru",
                                "savoy",
                                "dalton",
                                "pittsfield",
                                "florida",
                                "clarksburg",
                                "adams",
                                "north adams"
                            ]:
            local_housing_court = "Western Housing Court - Pittsfield Session"
        elif address_to_compare.city.lower() in ['agawam', 'blandford', 'brimfield', 'chester', 'chicopee', 'east longmeadow', 'granville', 'hampden', 'holland', 'holyoke', 'longmeadow', 'ludlow', 'monson', 'montgomery', 'palmer', 'russell', 'southwick', 'springfield', 'tolland', 'wales', 'west springfield', 'westfield','wilbraham']:
            local_housing_court = "Western Housing Court - Springfield Session"
        elif address_to_compare.city.lower() in ['charlton', 'dudley', 'oxford', 'southbridge', 'sturbridge', 'webster']:
            local_housing_court ="Central Housing Court - Dudley Session"
        elif address_to_compare.city.lower() in ['ashburnham', 'athol', 'fitchburg', 'gardner', 'holden', 'hubbardston', 'leominster', 'lunenburg', 'petersham', 'phillipston', 'princeton', 'royalston', 'templeton', 'westminster', 'winchendon']:
            local_housing_court = "Central Housing Court - Leominster Session"
        elif address_to_compare.city.lower() in ['ashland', 'berlin', 'bolton', 'framingham', 'harvard', 'holliston', 'hopkinton', 'hudson', 'marlborough', 'natick', 'northborough', 'sherborn', 'southborough', 'sudbury', 'wayland', 'westborough']:
            local_housing_court = "Central Housing Court - Marlborough Session"
        elif address_to_compare.city.lower() in ['auburn', 'barre', 'bellingham', 'blackstone', 'boylston', 'brookfield', 'clinton', 'douglas', 'east brookfield', 'grafton', 'hardwick', 'hopedale', 'lancaster', 'leicester', 'mendon', 'milford', 'millbury', 'millville', 'new braintree', 'northbridge', 'north brookfield', 'oakham', 'oxford', 'paxton', 'rutland', 'shrewsbury', 'spencer', 'sterling', 'sutton', 'upton', 'uxbridge', 'warren', 'west boylston', 'worcester',"west brookfield","w. brookfield"]:
            local_housing_court = "Central Housing Court - Worcester Session"
        elif address_to_compare.city.lower() in ['abington', 'avon', 'bellingham', 'braintree', 'bridgewater', 'brockton', 'canton', 'cohasset', 'dedham', 'dover', 'east bridgewater', 'eastham', 'foxborough', 'franklin', 'holbrook', 'medfield', 'medway', 'millis', 'milton', 'needham', 'norfolk', 'norwood', 'plainville', 'quincy', 'randolph', 'sharon', 'stoughton', 'walpole', 'wellesley', 'west bridgewater', 'westwood', 'weymouth', 'whitman', 'wrentham']:
            local_housing_court = "Metro South Housing Court - Brockton Session"
        elif address_to_compare.county.lower() == "norfolk county" and not address_to_compare.city.lower() in ["newton","brookline"]:
            local_housing_court = "Metro South Housing Court - Canton Session"
        elif address_to_compare.city.lower() in ['amesbury', 'andover', 'boxford', 'georgetown', 'groveland', 'haverhill', 'lawrence', 'merrimac', 'methuen', 'newbury', 'newburyport', 'north andover', 'rowley', 'salisbury', 'west newbury']:
            local_housing_court =  "Northeast Housing Court - Lawrence Session"
        elif address_to_compare.city.lower() in ['acton', 'ashby', 'ayer', 'billerica', 'boxborough', 'carlisle', 'chelmsford', 'devens', 'dracut', 'dunstable', 'groton', 'littleton', 'lowell', 'maynard', 'pepperell', 'shirley', 'stow', 'tewksbury', 'townsend', 'tyngsborough', 'westford']:
            local_housing_court = "Northeast Housing Court - Lowell Session"
        elif address_to_compare.city.lower() in ['lynn', 'nahant', 'saugus']:
            local_housing_court = "Northeast Housing Court - Lynn Session"
        elif address_to_compare.city.lower() in ['beverly', 'danvers', 'essex', 'gloucester', 'hamilton', 'ipswich', 'lynnfield', 'manchester-by-the-sea', 'marblehead', 'middleton', 'peabody', 'rockport', 'salem', 'swampscott', 'topsfield', 'wenham']:
            local_housing_court = "Northeast Housing Court - Salem Session"
        elif address_to_compare.city.lower() in ['bedford', 'burlington', 'concord', 'everett','lexington', 'lincoln', 'malden', 'melrose', 'north reading', 'reading', 'stoneham', 'wakefield', 'waltham', 'watertown', 'weston', 'wilmington', 'winchester', 'woburn']:
            local_housing_court = "Northeast Housing Court - Woburn Session"
        elif address_to_compare.city.lower() in ['freetown', 'westport', 'fall river', 'somerset','swansea']:
            local_housing_court = "Southeast Housing Court - Fall River Session"
        elif address_to_compare.city.lower() in ['acushnet', 'dartmouth', 'fairhaven', 'freetown', 'new bedford','westport']:
            local_housing_court = "Southeast Housing Court - New Bedford Session"
        elif address_to_compare.county.lower() in ["barnstable county", "dukes county","nantucket county"]:
            local_housing_court = "Southeast Housing Court - Barnstable session"
        elif address_to_compare.city.lower() in [
                "barnstable", "bourne", "brewster", "chatham", "dennis", "eastham", "falmouth", "harwich", "mashpee", "orleans", "provincetown", "sandwich", "yarmouth",
                "aquinnah", "chilmark", "edgartown", "oak bluffs", "west tisbury",
                "nantucket"
        ]:
            local_housing_court = "Southeast Housing Court - Barnstable Session"
        elif address_to_compare.city.lower() in ['gosnold','aquinnah', 'barnstable', 'bourne', 'brewster', 'carver', 'chatham', 'chilmark', 'dennis', 'duxbury', 'edgartown', 'falmouth', 'halifax', 'hanson', 'harwich', 'kingston', 'lakeville', 'marion', 'marshfield', 'mashpee', 'mattapoisett', 'middleborough', 'nantucket', 'oak bluffs', 'pembroke', 'plymouth', 'plympton', 'provincetown', 'rochester', 'sandwich', 'wareham', 'accord', 'assinippi', 'hanover', 'hingham', 'hull', 'humarock', 'norwell', 'rockland', 'scituate',"tisbury"]:
            local_housing_court = "Southeast Housing Court - Plymouth Session"
        elif address_to_compare.city.lower() in ['attleboro', 'berkley', 'dighton', 'easton', 'mansfield', 'north attleborough', 'norton', 'raynham', 'rehoboth', 'seekonk','taunton']:
            local_housing_court = "Southeast Housing Court - Taunton Session"
        else:
            local_housing_court = ""
        
        # Try one time to match the normalized address instead of the 
        # literal provided address if first match fails
        if depth == 0 and not local_housing_court and hasattr(address, "norm_long"):
            return self.matching_housing_court_name(address.norm_long, depth=1)
        return local_housing_court

    def matching_bmc(self, address: Address) -> Optional[MACourt]:
        if address.city.lower() in ["winthrop"]:
            # This city is not in Boston but is served by East Boston BMC
            court_name = "East Boston Division, Boston Municipal Court"
        else:
            try:
                court_name = self.get_boston_ward_number(address)[1] + ' Division, Boston Municipal Court'
            except:
                return None
        return next((court for court in self.elements if court.name.rstrip().lower() == court_name.lower()), None)

    def load_boston_wards_from_file(self, json_path, data_path:Optional[str]=None) -> GeoDataFrame:
        """load geojson file for boston wards"""
        if data_path is None:
          if hasattr(self, 'data_path'):
            data_path = self.data_path
          else:
            data_path = 'docassemble.MACourts:data/sources/'
        path = path_and_mimetype(os.path.join(self.data_path, json_path+'.geojson'))[0]
        if path is None:
          # fallback, for running on non-docassemble (i.e. unit tests)
          path = os.path.join(self.data_path, json_path+'.geojson')
        wards = gpd.read_file(path)
        
        return wards

    def get_boston_ward_number(self, address: Address) -> Tuple[str, str]:
        """
        This function takes an address object as input,
        filters a geojson file to only include the ward
        that contains the address, and returns the
        ward number and name of the courthouse.
        If the address object doesn't have loaction
        data, it will return an empty string.
        If the address location is not in Boston,
        it will return and empty string.
        If the address location is in Boston, but
        not within a ward boundary, return the
        closest ward.
        Dependencies:
        1.Geopandas for loading the geojson file
        2.Shapely for constructing Point object
        """
        
        #if location data is not in address object, return empty string
        if (not hasattr(address, 'location')):
            return '',''

        #if location is in Boston, lookup ward
        elif address.norm.city.lower() in ['boston','east boston','charlestown']:
            #assign point object
            p1 = Point(address.location.longitude, address.location.latitude)

            #load geojson Boston Ward map
            boston_wards = self.load_boston_wards_from_file(json_path = "boston_wards")

            #find ward containing point object
            ward = boston_wards[[p1.within(boston_wards.geometry[i]) for i in range(len(boston_wards))]]

            #if result exists, return result
            if len(ward) > 0:
                ward_number = ward.iloc[0].Ward_Num
                courthouse_name = ward.iloc[0].courthouse
                
                return ward_number, courthouse_name

            #else find closest ward and return result
            else:
                distances_to_wards = [p1.distance(boston_wards.iloc[i].geometry) for i in range(len(boston_wards))]
                mask = [i==min(distances_to_wards) for i in distances_to_wards]
                ward = boston_wards[mask]

                ward_number = ward.iloc[0].Ward_Num
                courthouse_name = ward.iloc[0].courthouse
                
                return ward_number, courthouse_name

        #if location in not in Boston, return empty string
        else:
            return '',''

    ####################################
    ## Docket number parser (i.e. smart docket tool)

    _find_court_code_re = re.compile(r'(?<=^\d{2})(\d{2}|H\d{2})(?!-)|^[A-Z]{2}'
                                    r'(?=\d{2})', re.I)

    _find_case_type_code_re = re.compile(r'(?<!^)[A-Z]{2,4}(?!-)', re.I)

    _any_numbers_re = re.compile(r'\d', re.I)

    _court_case_type_code_dict = {
        'AC' : 'Application for Criminal Complaint',
        'AD' : 'Appeal',
        'BP' : 'Bail Petition',
        'CI' : 'Civil Infraction',
        'CR' : 'Criminal',
        'CV' : 'Civil',
        'IC' : 'Interstate Compact',
        'IN' : 'Inquest',
        'MH' : 'Mental Health',
        'MV' : 'Motor Vehicle',
        'PC' : 'Probable Cause',
        'RO' : 'Abuse Prevention Order',
        'SC' : 'Small Claims',
        'SP' : 'Supplementary Process',
        'SU' : 'Summary Process',
        'SW' : 'Administrative Search Warrant',
        'TK' : 'Ticket Hearings',
        'PS' : 'Permit Session',
        'SM' : 'Service Members',
        'TL' : 'Tax Lien',
        'REG': 'Registration',
        'SBQ': 'Subsequent',
        'MISC': 'Miscellaneous'
    }

    _land_court_case_type_code_dict = {
        'PS' : 'Permit Session',
        'SM' : 'Service Members',
        'TL' : 'Tax Lien',
        'REG': 'Registration',
        'SBQ': 'Subsequent',
        'MISC': 'Miscellaneous'
    }

    _probate_family_court_case_type_code_dict = {
        'AB' : 'Protection from Abuse',
        'AD' : 'Adoption',
        'CA' : 'Change of Name',
        'CS' : 'Custody, Support, and Parenting Time',
        'CW' : 'Child Welfare',
        'DO' : 'Domestic Relations, Other',
        'DR' : 'Domestic Relations',
        'EA' : 'Estates and Administration',
        'GD' : 'Guardianship',
        'JP' : 'Joint Petition',
        'PE' : 'Paternity in Equity',
        'PM' : 'Probate Abuse / Conservator',
        'PO' : 'Probate, Other',
        'PP' : 'Equity-Partition',
        'QC' : 'Equity Complaint',
        'QP' : 'Equity Petition',
        'SK' : 'Wills for Safekeeping',
        'WD' : 'Paternity',
        'XY' : 'Proxy Guardianship'
    }

    _probate_family_court_case_group_code_dict = {
        'A' : 'Adoption',
        'C' : 'Change of Name',
        'D' : 'Domestic Relations',
        'E' : 'Equity',
        'W' : 'Paternity',
        'P' : 'Probate',
        'R' : 'Protection from Abuse',
        'X' : 'Proxy Guardianship',
        'S' : 'Wills for Safekeeping'
    }

    # map from these court codes in docket numbers to the real court codes
    _alt_court_codes = {
        'BA' : 'P72', # Barnstable Probate and Family Court
        'BR' : 'P73', # Bristol Probate and Family Court
        'DU' : 'P74', # Dukes Probate and Family Court
        'BE' : 'P76', # Berkshire Probate and Family Court'
        'ES' : 'P77', # Essex Probate and Family Court
        'FR' : 'P78', # Franklin Probate and Family Court
        'HD' : 'P79', # Hampden Probate and Family Court
        'HS' : 'P80', # Hampshire Probate and Family Court
        'MI' : 'P81', # Middlesex Probate and Family Court
        'NA' : 'P75', # Nantucket Probate and Family Court
        'NO' : 'P82', # Norfolk Probate and Family Court
        'PL' : 'P83', # Plymouth Probate and Family Court
        'SU' : 'P84', # Suffolk Probate and Family Court
        'WO' : 'P85', # Worcester Probate and Family Court
    }

    _appellate_court_code_dict = {
        'P'  : 'Appeals Court (Panel)',
        'J'  : 'Appeals Court (Single Justice)',
    }

    _sjc_code_dict = {
        'SJC': 'Supreme Judicial Court',
        'SJ' : 'Supreme Judicial Court (Single Justice)',
        'BD' : 'Supreme Judicial Court (Bar Docket)'
    }

    def courts_from_docket_number(self, docket_number:str) -> List[MACourt]:
        """Gets the court objects that matches a given docket number. There
        will be one object per court session (physical location)"""
        search = self._find_court_code_re.search(docket_number)
        court_code = search.group() if search else None
        if not court_code:
            for key in self._land_court_case_type_code_dict:
                if key in docket_number.upper():
                    only_land_court = self.matching_land_court(None)
                    if only_land_court:
                      return [only_land_court]
                    else:
                      return []
            else:
                if not self._any_numbers_re.search(docket_number):
                    raise KeyError(f"{docket_number} doesn't have any number digits in it, it's not likely a docket number")
                for key in self._sjc_code_dict:
                    if key in docket_number.upper():
                        # TODO(brycew): integrate this into the MA Courts properly
                        court = MACourt()
                        court.name = self._sjc_code_dict[key]
                        court.court_code = key
                        # TODO(brycew): there are several more SJC tyler codes:
                        # [sjcab, sjc:commar, sjc:commar2, sjc:commfc, sjc:commonwealth2, sjc:fullcoruts2,
                        #  sjc:pab, sjc:singlej, sjc:suffolk]. Do each need to be distinguished?
                        court.tyler_code = 'sjc'
                        court.description = 'The Supreme Judicial Court of Massachusetts'
                        return [court]
                for key, name in self._appellate_court_code_dict.items():
                    if key in docket_number.upper():
                        matching_courts = [court for court in self.elements if name.lower() in court.name.rstrip().lower()]
                        if matching_courts:
                            return matching_courts
                else:
                    raise KeyError(f"{docket_number} doesn't have a court code, and isn't an appellate case, might be a variant")
                    # The docket number is missing court code. Currently, because
                    # the initial check_proper_format check excludes docket numbers
                    # missing court codes (or at least what appears to be court
                    # codes), this part is superfluous but won't be once the code
                    # is edited to address variations, which might not include
                    # court codes.
        else:
            if court_code.upper() in self._alt_court_codes:
              search_court_code = self._alt_court_codes[court_code.upper()]
            else:
              search_court_code = court_code
            matching_courts = [court for court in self.elements if court.court_code.strip().lower() == str(search_court_code).lower()]
            if not matching_courts:
              raise KeyError(f"{court_code} (i.e. {search_court_code}) (from {docket_number}) isn't a valid court code")
            return matching_courts

    def court_from_docket_number(self, docket_number:str) -> Optional[MACourt]:
        """Returns only the information that is the same between different court sessions for that docket_number
        (i.e., name, court_code, and description)"""
        matching_courts = self.courts_from_docket_number(docket_number)
        court = MACourt()
        court.name = matching_courts[0].name if len(set([c.name for c in matching_courts])) == 1 else None
        court.court_code = matching_courts[0].court_code if len(set([c.court_code for c in matching_courts])) == 1 else None
        court.tyler_code = matching_courts[0].tyler_code if len(set([c.tyler_code for c in matching_courts])) == 1 else None
        court.description = matching_courts[0].description if len(set([c.description for c in matching_courts])) == 1 else None
        if court.name is None and court.court_code is None and court.description is None:
            return None
        return court

    def case_type_from_docket_number(self, docket_number:str) -> Optional[str]:
        court = self.court_from_docket_number(docket_number)
        if not court:
          return None
        search = self.find_case_type_code_re.search(docket_number)
        case_type_code = search.group() if search else None
        if not case_type_code:
            if 'appeals' in court.name.lower() or 'supreme' in court.name.lower():
                return 'Appellate'
                # Without the docket number for the case in the lower court, we
                # cannot discern the case type. Not using the case-type code 'AD'
                # ('Appeal') here because 'AD' is a case-type code for trial,
                # rather than appellate, courts.
            return None
            # The docket number is missing case-type code. See above comment in
            # identify_court_name function re check_proper_format and variations.
        else:
            if 'probate' in court.name.lower():
                for key in self._probate_family_court_case_type_code_dict:
                    if key == case_type_code:
                        return self._probate_family_court_case_type_code_dict[key]
                raise Exception

            # Case-type identification separates Probate and Family Court from other
            # courts because 'AD' refers to 'Adoption' in Probate and Family Court
            # while it refers to 'Appeal' in others.
            else:
                for key in self._court_case_type_code_dict:
                    if key == case_type_code:
                        return self._court_case_type_code_dict[key]
                raise Exception
                # The docket number has incorrect (nonexistent) case-type code.

find_year_re = re.compile(r'^\d{2}(?=\d{2}[A-Z]|H|\s)|(?<=^[A-Z]{2})\d{2}|'
                          r'\d{4}(?=-)|\d{2}(?=-)', re.I)

def get_year_from_docket_number(docket_number:str) -> Optional[str]:
    search = find_year_re.search(docket_number)
    case_year = search.group() if search else None
    if not case_year:
        return None
        # Remember: except for panel SJC docket numbers, ALL others, incl.
        # variations, include the year.
    else:
        if case_year[-2:] > time.strftime('%y'):
            raise ValueError(f'docket number has a year in the future: {case_year}')
            # The docket number has incorrect year: it refers to a case that has
            # not yet been filed, i.e., a case that does not exist.
        else:
            if len(case_year) == 4:
                return case_year
            else:
                return time.strftime('%Y')[:2] + case_year

find_sequence_number_re = re.compile(r'\d{2,}(?=$|[A-Z]+$)', re.I)

def get_sequence_number_from_docket_number(docket_number:str) -> Optional[str]:
    search = find_sequence_number_re.search(docket_number)
    return search.group() if search else None

def parse_division_from_name(court_name) -> str:
    rules = {
        "District Court": r'(.*)( District Court)',
        "Boston Municipal Court": r"(.*)(, Boston Municipal Court)",
        "Housing Court": r"(.*)( Housing Court)",
        "Superior Court": r"(.*)( Superior Court)",
        "Juvenile Court": r"(.*)( Juvenile Court)",
        "Land Court": r"(Land Court)",
        "Probate and Family": r"(.*)( Probate and Family Court)",}
    for key in rules:
        match = re.match(rules[key], court_name)
        if match:
            return match[1] # We need to make sure the regex has a group though
            # if len(match) > 1:
            #     return match[1]
            # else:
            #     return match[0]
    # court.department = item['name']
    return court_name

class MAPlace(DAObject):
    def init(self, *pargs, **kwargs):
        super(MAPlace, self).init(*pargs, **kwargs)
        if 'address' not in kwargs:
            self.initializeAttribute('address', Address)
        if 'location' not in kwargs:
            self.initializeAttribute('location', LatitudeLongitude)
    def _map_info(self):
        if not hasattr(self,'description'):
            self.description = str(self)
        the_info = self.description
        the_info += "  [NEWLINE]  " + self.address.block()
        result = {'latitude': self.location.latitude, 'longitude': self.location.longitude, 'info': the_info}
        if hasattr(self, 'icon'):
            result['icon'] = self.icon
        return [result]

@prevent_dependency_satisfaction
def combined_locations(locations):
    """Accepts a list of locations, and combines locations that share a
    latitude/longitude in a way that makes a neater display in Google Maps.
    Designed for MACourts class but may work for other objects that subclass DAObject.
    Will not work for base Address class but should never be needed for that anyway
    Rounds lat/longitude to 3 significant digits
    """

    places = list()
    def has_match(locations, other) -> bool:
        for item in locations:
            if match(item, other):
                return True
        return False

    def match(item, other) -> bool:
        if not item is None and not other is None:
            return (
                round(item.location.latitude, 3) == round(other.location.latitude, 3) and
                round(item.location.longitude, 3) == round(other.location.longitude, 3)
            )
        return False

    for location in locations:
        if isinstance(location, DAObject):
            if not has_match(places,location):
                places.append(MAPlace(location=location.location, address=copy.deepcopy(location.address), description = str(location)))
            else:
                for place in places:
                    if match(place,location):
                        if hasattr(place, 'description') and str(location) not in place.description:
                            place.description += "  [NEWLINE]  " + str(location)
    return places