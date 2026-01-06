# do not pre-load
from pathlib import Path

import unittest
from hypothesis import given, strategies as st
from docassemble.base.util import Address, LatitudeLongitude

from ..macourts import MACourtList

class TestCourtFinder(unittest.TestCase):
    def setUp(self):
      data_path = data_path=Path(__file__).resolve().parent.joinpath('../data/sources')
      self.all_courts = MACourtList(data_path=data_path)
      self.all_courts.load_courts(
          ['housing_courts','bmc','district_courts','superior_courts','land_court','juvenile_courts','probate_and_family_courts','appeals_court', 'supreme_judicial_court'],
      )
      self.court_types = ["District Court", "Boston Municipal Court","Housing Court","Superior Court", "Probate and Family Court","Juvenile Court","Land Court", "Supreme Judicial Court", "Appeals Court"]

    def test_search_in_bristol(self):
        address = Address(address="91 Highland Road", city="Swansea", state="Massachusetts", county="Bristol County", zip="02777")
        court_list = self.all_courts.matching_probate_and_family_court(address)
        self.assertGreaterEqual(len(court_list), 3)
        self.assertIn("Bristol Probate and Family Court",
            [
                court.name
                for court
                in court_list
            ], 
        )

    def test_search_boston(self):
        address = Address(address="1234 Soldiers Field Road", city="Boston", county="Suffolk County", 
            state="Massachusetts", zip="02135", 
            location=LatitudeLongitude(latitude=42.3641126, longitude=-71.1364048),
            norm=Address(address="1234 Soliders Field Road", city="Boston", county="Suffolk Courty", 
                state="Massachusetts", zip="02135"))
        court_list = self.all_courts.matching_courts(address, court_types=self.court_types)
        court_strings = [str(court.name) for court in court_list]
        self.assertEqual(len(court_list), 8) # Added SJC and Appeals Court
        self.assertIn("Brighton Division, Boston Municipal Court", court_strings)
        self.assertIn("Suffolk County Superior Court", court_strings)
        self.assertIn("Eastern Housing Court", court_strings)
        self.assertIn("Suffolk Probate and Family Court", court_strings)
        self.assertIn("Boston Juvenile Court", court_strings)
        self.assertIn("Land Court", court_strings)

    def test_find_sjc(self):
        address = Address(address="123 Main St", city="Anywhere", state="Massachusetts", zip="00000")
        court_list = self.all_courts.matching_courts(address, court_types=["Supreme Judicial Court"])
        self.assertEqual(len(court_list), 1)
        self.assertEqual(court_list[0].name, "Supreme Judicial Court")

    def test_random_points(self):
        # previously generated 65 random points that were within the state, and turned those into these addresses
        # dfd = gpd.GeoDataFrame.from_features(feature) # (rough outline of massachusetts)
        # x_min, y_min, x_max, y_max = df.total_bounds
        # x = np.random.uniform(x_min, x_max, 100)
        # y = np.random.uniform(y_min, y_max, 100)
        # gpd_points = gpd.GeoSeries(gpd.points_from_xy(x, y))
        # gpd_points = gpd_points[gpd_points.within(df.unary_union)]
        # geolocator = Nominatim(user_agent="suffolklitlab_manual")
        # for p in gpd_points:
        #   time.sleep(5)
        #   loc = geolocator.reverse(f"{p.y}, {p.x}")
        #   l = loc.raw['address']
        #   print(f"('{l.get('house_number', ''} {l.get('road', '')}', '{l.get('town', l.get('city', ''))}', '{l.get('county')}', '{l.get('postcode')}', {loc.latitude}, {loc.longitude},")
        for street, city, county, zip, latitude, longitude in (
           (' Highland Commons West', 'Berlin', 'Worcester County', '01503', 42.3939894991329, -71.60000206953706),
            (' Tremont Street', 'Dighton', 'Bristol County', '02764', 41.8586440083136, -71.15159239168555),
            (' Evergreen Street', 'Southbridge', 'Worcester County', '01550', 42.0799458, -72.02040228534332),
            ('18 Elm Street', 'Freetown', 'Bristol County', '02702', 41.79514055, -71.0633314452663),
            ('129 Sargent Street', 'Belchertown', 'Hampshire County', '01007', 42.28715805, -72.39633334734003),
            ('22 Franklin Hill Road', 'Colrain', 'Franklin County', '01340', 42.71362463617108, -72.70252143052237),
            (' Wheelwright Road', 'Barre', 'Worcester County', '01094', 42.364424, -72.12890951218827),
            (' Dudley Road', 'Templeton', 'Worcester County', '01468', 42.5400706, -72.08168418092261),
            (' I 93', 'Boston', 'Suffolk County', '02145', 42.38474243059918, -71.07666832743206),
            (' Yankee Division Highway', 'Needham', 'Norfolk County', '02494', 42.27724069585515, -71.19971040435486),
            (' Foundry Street', 'Easton', 'Bristol County', '02375', 42.01411245, -71.1035188918578),
            ('48 Tor Court', 'Pittsfield', 'Berkshire County', '01202', 42.454234, -73.28252945853066),
            ('25 Adams Street', 'Medway', 'Norfolk County', '02053', 42.1545494, -71.43164025989681),
            ('415 Brookfield Road', 'Brimfield', 'Hampden County', '01010', 42.1593942, -72.15071408281906),
            ('415 Center Street', 'Bridgewater', 'Plymouth County', '02325', 41.9902627, -70.99361821983221),
            ('26 Broadway Street', 'Westford', 'Middlesex County', '01886', 42.5928758, -71.46285875510262),
            (' Central Street', 'Holliston', 'Middlesex County', '02054', 42.19507608106604, -71.39058839757578),
            ('222 Concord Road', 'Lincoln', 'Middlesex County', '01733', 42.410906600000004, -71.3426944),
            ('106 Howland Road', 'Freetown', 'Bristol County', '02347', 41.7977477, -71.02534964514375),
            ('55 Pinehurst Avenue', 'Pittsfield', 'Berkshire County', '01201', 42.4840195, -73.25331502805794),
            ('61 Smith Street', 'Dover', 'Norfolk County', '02030', 42.22700093875156, -71.33199715588512),
            ('65 Harlow Clark Road', 'Huntington', 'Hampshire County', '01052', 42.2458936, -72.84830370713917),
            (' Spencer Road', 'Oakham', 'Worcester County', '01068', 42.32618724167509, -72.02539362554955),
            ('25 Circle View Drive', 'Hampden', 'Hampden County', '01036', 42.075583300000005, -72.45642812801954),
            ('175 Montague Road', 'Shutesbury', 'Franklin County', '01072', 42.4624147, -72.42725758663953),
            ('10 Donald Road', 'Hamilton', 'Essex County', '01936', 42.62015785, -70.82576720046993),
            ('10 Catamount Hill Road Number One', 'Colrain', 'Franklin County', '01340', 42.674503697774504, -72.74969239072699),
            ('10 Purgatory Road', 'Sutton', 'Worcester County', '01590', 42.13648085, -71.74179041306819),
            ('4 Paige Road', 'Burlington', 'Middlesex County', '01803', 42.493098700000004, -71.17038274317028),
            ('60 Carriage House Lane', 'Wrentham', 'Norfolk County', '02093', 42.04160195, -71.37390149442945),
            (' Pine Hill Road', 'Orange', 'Franklin County', '01364', 42.627378, -72.30768474344747),
            ('7 Galloway Road', 'Chelmsford', 'Middlesex County', '01863', 42.5906206, -71.39816569656239),
            ('125 Maximilian Drive', 'Granby', 'Hampshire County', '01033', 42.27031635, -72.48946252987403),
            ('15 Chanterwood Road', 'Lee', 'Berkshire County', '01264', 42.28536405, -73.19914315),
            ('1576 Shirley Road', 'Lancaster', 'Worcester County', '01464', 42.51381365, -71.66523721038719),
            ('85 Meaghan Circle', 'Taunton', 'Bristol County', '02347', 41.853822199999996, -70.99174316201103),
            ('46 Morse Road', 'Holland', 'Hampden County', '01521', 42.08623088769841, -72.16216836352413),
            (' Partridge Hill Road', 'Charlton', 'Worcester County', '01571', 42.088394550000004, -71.93787017396735),
            ('444 East Windsor Road', 'Windsor', 'Berkshire County', '01270', 42.49057795, -73.02528131660799),
            ('33 Shaws Lane', 'Springfield', 'Hampden County', '01104', 42.12966305, -72.56302424924249),
            ('West Sturbridge Road', 'East Brookfield', 'Worcester County', '01506', 42.182487449999996, -72.05902183935248),
            ('40 Maureen Way', 'Millville', 'Worcester County', '01529', 42.054910050000004, -71.58685925),
            ('14 Kimball Avenue', 'Wenham', 'Essex County', '01984', 42.60853335, -70.9096519548315),
            ('356 East Street', 'Sharon', 'Norfolk County', '02067', 42.1259197, -71.15284198942624),
            ('Yankee Division Highway', 'Quincy', 'Norfolk County', '02368', 42.2161895, -71.0523257),
            ('71 Bligh Street', 'Tewksbury', 'Middlesex County', '01876', 42.62888615, -71.20285122390203),
            ('175 Millwood Street', 'Framingham', 'Middlesex County', '01701', 42.32151435, -71.4593585337083),
            ('50 Lorraine Drive', 'North Adams', 'Berkshire County', '01247', 42.709677400000004, -73.10311456300539),
            ('Main Road', 'Tyringham', 'Berkshire County', '01264', 42.2769872, -73.22355983545783),
            ('15 Randall Road', 'Rochester', 'Plymouth County', '02770', 41.77068965, -70.82974667822991),
            ('8 Anthony Drive', 'Hinsdale', 'Berkshire County', '01235', 42.400403, -73.1225440305746),
            ('Primary Trail', 'Westford', 'Middlesex County', '08163', 42.62669499516589, -71.42622469506479),
            ('Ledges Trail', 'Petersham', 'Worcester County', '01366', 42.4943804, -72.1723491),
            ('Loop Trail', 'Boylston', 'Worcester County', '01505', 42.36290785, -71.72461856990755),
            ('3 Samual Harrington Road', 'Westborough', 'Worcester County', '01581', 42.29925195, -71.58548016591237),
            ('1099 Ashby West Road', 'Fitchburg', 'Worcester County', '01431', 42.63033245, -71.83718909827459),
            ('58 Ring Road', 'Kingston', 'Plymouth County', '02364', 41.98339745, -70.76900261113018),
        ):
            address = Address(address=street, city=city, county=county, state="Massachusetts", zip=zip)
            address.norm_long = address
            address.norm = address
            address.location = LatitudeLongitude(latitude=latitude, longitude=longitude),
            court_list = self.all_courts.matching_courts(address, court_types=self.court_types)
            self.assertGreaterEqual(len(court_list), 1)


    @given(address=st.text(), city=st.text(), county=st.text(), zip=st.from_regex(r"[0-9]{5}"))
    def test_search_all(self, address, city, county, zip):
        address = Address(address=address, city=city, county=county, state="Massachusetts", zip=zip)
        address.norm_long = address
        address.norm = address
        self.all_courts.matching_courts(address, court_types=self.court_types)


if __name__ == "__main__":
    unittest.main()