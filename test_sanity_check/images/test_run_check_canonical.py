#!/usr/bin/env python
# coding: utf-8 
# created on 2018.03.31 using PyCharm 
# project impresso-image-acquisition

from unittest import TestCase
import unittest

from images import check_images
from impresso_commons import path

__author__ = "maudehrmann"


class TestRunCheckCanonical(TestCase):
    global orig_dir
    global canon_dir
    orig_dir = "../../test-data/original"
    canon_dir = "../../test-data/canonical"

    # ref data for corpus 1
    canonical_cases_one = {'issues w/o jp2': ['TESTIMGCANOONE/1900/01/13/a'],
                           'issues w/ incorrect # infofile': [],
                           'pages w/o jp2': ['TESTIMGCANOONE/1900/01/11/9'],
                           'issues w/o infofile': ['TESTIMGCANOONE/1900/01/12/a'],
                           'infofile w/ incorrect # img': [],
                           'jp2 w/ incorrect filename': [],
                           'jp2 w/o journalname': ['TESTIMGCANOONE/1900/01/11/a/GDL-1900-01-11-a-p0001.jp2'],
                           'infofile w/o journalname': ['TESTIMGCANOONE/1900/01/11/a/1900-01-11-a-image-info.txt'],
                           'jp2 w/ incorrect date': [],
                           'infofile w/ incorrect date': []}

    images_counts_one = {'number tif': 8,
                         'number png': 8,
                         'number jpg': 0,
                         'total size jp2': 214552196,
                         'number original page folders': 21,
                         'number canonical jp2': 20}

    journal_counts_one = {'number original issues': 5,
                          'number canonical issues': 5,
                          'number recognized pairs': 5,
                          'issues w/o zip': 0,
                          'issues w/ corruptedzip': 0}

    # ref data for corpus 2
    canonical_cases_two = {'issues w/o jp2': ['TESTIMGCANOTWO/1900/01/13/a'],
                           'issues w/ incorrect # infofile': [],
                           'pages w/o jp2': ['TESTIMGCANOTWO/1900/01/11/9'],
                           'issues w/o infofile': ['TESTIMGCANOTWO/1900/01/12/a'],
                           'infofile w/ incorrect # img': ['TESTIMGCANOTWO/1900/01/11/a/1900-01-11-a-image-info.txt'],
                           'jp2 w/ incorrect filename': [],
                           'jp2 w/o journalname': ['TESTIMGCANOTWO/1900/01/11/a/JDG-1900-01-11-a-p0001.jp2', 'TESTIMGCANOTWO/1900/01/11/a/GDL-1900-01-11-a-p0001.jp2'],
                           'infofile w/o journalname': ['TESTIMGCANOTWO/1900/01/13/a/TESTIMGCANO2-1900-01-13-a-image-info.txt'],
                           'jp2 w/ incorrect date': [], 'infofile w/ incorrect date': []}

    images_counts_two = {'number tif': 8,
                         'number png': 8,
                         'number jpg': 0,
                         'total size jp2': 237479561,
                         'number original page folders': 21,
                         'number canonical jp2': 21}

    journal_counts_two = {'number original issues': 5,
                          'number canonical issues': 5,
                          'number recognized pairs': 5,
                          'issues w/o zip': 0,
                          'issues w/ corruptedzip': 0}

    maxDiff = None

    def test_run_check_canonical(self):
        one = "TESTIMGCANOONE"
        two = "TESTIMGCANOTWO"

        # test 1
        one_original_issues = path.detect_journal_issues(orig_dir, one)
        one_canonical_issues = path.detect_canonical_issues(canon_dir, one)

        # check
        res_journal_canonical_cases_one, res_image_counts_one, res_journal_counts_one = check_images.check_canonical_journal(
            one_original_issues, one_canonical_issues, None)

        self.assertDictEqual(self.canonical_cases_one, res_journal_canonical_cases_one)
        self.assertDictEqual(self.images_counts_one, res_image_counts_one)
        self.assertDictEqual(self.journal_counts_one, res_journal_counts_one)

        # test 2
        two_original_issues = path.detect_journal_issues(orig_dir, two)
        two_canonical_issues = path.detect_canonical_issues(canon_dir, two)

        # check
        res_journal_canonical_cases_two, res_image_counts_two, res_journal_counts_two = check_images.check_canonical_journal(
            two_original_issues, two_canonical_issues, None)

        self.assertDictEqual(self.canonical_cases_two, res_journal_canonical_cases_two)
        self.assertDictEqual(self.images_counts_two, res_image_counts_two)
        self.assertDictEqual(self.journal_counts_two, res_journal_counts_two)


if __name__ == '__main__':
    unittest.main()
