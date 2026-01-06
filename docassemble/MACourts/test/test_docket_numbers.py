import unittest
from ..macourts import *
from pathlib import Path

class TestDocketNumbers(unittest.TestCase):
  def setUp(self):
    self.all_courts = MACourtList()
    self.all_courts.load_courts(
        ['housing_courts','bmc','district_courts','superior_courts','land_court','juvenile_courts','probate_and_family_courts','appeals_court', 'supreme_judicial_court'],
        data_path=Path(__file__).resolve().parent.joinpath('../data/sources'))

  def _check_court_name(self, docket_number, expected_name):
    self.assertEqual(self.all_courts.court_from_docket_number(docket_number).name, expected_name)

  def _check_court_names(self, docket_number, expected_names):
    matching_courts = self.all_courts.courts_from_docket_number(docket_number)
    matching_names = [c.name for c in matching_courts]
    self.assertEqual(len(expected_names), len(matching_names), matching_names)
    for exp_name in expected_names:
      self.assertIn(exp_name, matching_names)

  def _expect_court_not_found(self, docket_number):
    try:
      courts = self.all_courts.courts_from_docket_number(docket_number)
      self.fail(f"{docket_number} shouldn't be a valid number, but it found {[str(c) for c in courts]}")
    except KeyError:
      # All good
      pass

  def _check_case_year(self, docket_number, expected_year):
    self.assertEqual(get_year_from_docket_number(docket_number), expected_year)

  def test_parse_docket_numbers(self):
    """From the examples"""
    self._check_court_name('1577CV00982', 'Essex County Superior Court')
    self._check_court_name('1577cv00982', 'Essex County Superior Court')
    self._check_court_name('1670CV000072', 'Winchendon District Court')
    self._check_court_name('1401CV001026', 'Central Division, Boston Municipal Court')
    # The Regex doesn't yet work with the "variations". Some are ambigious, but
    # the below ones aren't, and we should probably match them as well
    #self._check_court_name('1577-CV-00982', 'Essex County Superior Court')
    #self._check_court_name('1670-CV-000072', 'Winchendon District Court')
    #self._check_court_name('1401-CV-001026', 'Central Division, Boston Municipal Court')
    self._check_court_names('15H84CV000436', ['Eastern Housing Court',
      'Eastern Housing Court - Middlesex Session', 'Eastern Housing Court - Chelsea Session'])
    self._check_court_names('15h84cv000436', ['Eastern Housing Court',
      'Eastern Housing Court - Middlesex Session', 'Eastern Housing Court - Chelsea Session'])
    self._check_court_name('07 TL 001026', 'Land Court')
    self._check_court_names('ES15A0064AD', ['Essex Probate and Family Court', 'Lawrence Probate and Family Court'])
    self._check_court_names('es15A0064ad', ['Essex Probate and Family Court', 'Lawrence Probate and Family Court'])
    self._check_court_name('2020-P-0874', 'Massachusetts Appeals Court (Panel)')
    self._check_court_name('2020-p-0874', 'Massachusetts Appeals Court (Panel)')
    self._check_court_name('SJC-13103', 'Supreme Judicial Court')
    self._check_court_name('sjc-13103', 'Supreme Judicial Court')

    # Test some invalid ones too
    self._expect_court_not_found('')
    self._expect_court_not_found('complete gibberish')
    self._expect_court_not_found('123098120398213098123')
    self._expect_court_not_found('12')
    self._expect_court_not_found('9999CV00000')
    self._expect_court_not_found('1000-K-1234')

  def test_variants(self):
    # All of these variants should be '2015'
    # This variant isn't supported yet though
    #self._check_case_year('1577-CV-00982', '2015')
    self._check_case_year('1577CV00982', '2015')
    self._check_case_year('15-0982', '2015')
    self._check_case_year('15-CV-00982', '2015')
    self._check_case_year('2015-982', '2015')
    self._check_case_year('2015-00982', '2015')

    # These should be '2020', a harder one to parse
    self._check_case_year('2077CV00982', '2020')
    self._check_case_year('20-0982', '2020')
    self._check_case_year('20-CV-00982', '2020')
    self._check_case_year('2020-982', '2020')
    self._check_case_year('2020-00982', '2020')