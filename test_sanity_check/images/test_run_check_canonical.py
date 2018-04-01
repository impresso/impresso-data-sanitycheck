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
    global newspapers
    orig_dir = "../../test-data/original"
    canon_dir = "../../test-data/canonical"
    newspapers = "TESTIMGCANOONE TESTIMGCANOTWO"

    canonical_cases = {'issues w/o jp2': ['TESTIMGCANOTWO/1900/01/13/a'],
                       'issues w/ incorrect # infofile': [],
                       'pages w/o jp2': ['TESTIMGCANOTWO/1900/01/11/9'],
                       'issues w/o infofile': ['TESTIMGCANOTWO/1900/01/12/a'],
                       'infofile w/ incorrect # img': ['TESTIMGCANOTWO/1900/01/11/a/1900-01-11-a-image-info.txt'],
                       'jp2 w/ incorrect filename': [],
                       'jp2 w/o journalname': ['TESTIMGCANOTWO/1900/01/11/a/JDG-1900-01-11-a-p0001.jp2',
                                               'TESTIMGCANOTWO/1900/01/11/a/GDL-1900-01-11-a-p0001.jp2'],
                       'infofile w/o journalname': [
                           'TESTIMGCANOTWO/1900/01/13/a/TESTIMGCANO2-1900-01-13-a-image-info.txt'],
                       'jp2 w/ incorrect date': [],
                       'infofile w/ incorrect date': []}

    images_counts = {'number tif': 8,
                     'number png': 8,
                     'number jpg': 0,
                     'total size jp2': 214552196,
                     'number original page folders': 21,
                     'number canonical jp2': 20}

    journal_counts = {'number original issues': 5,
                      'number canonical issues': 5,
                      'number recognized pairs': 5,
                      'issues w/o zip': 0,
                      'issues w/ corruptedzip': 0}

    maxDiff = None

    def test_run_check_canonical(self):
        journals = newspapers.split(" ")

        for journal in journals:

            original_issues = path.detect_journal_issues(orig_dir, journal)
            canonical_issues = path.detect_journal_issues(canon_dir, journal)

            # check
            res_journal_canonical_cases, res_image_counts, res_journal_counts = check_images.check_canonical_journal(
                original_issues, canonical_issues, None)

            print(str(res_journal_canonical_cases))
            print(str(res_image_counts))
            print(str(res_journal_counts))

            self.assertDictEqual(self.canonical_cases, res_journal_canonical_cases)
            self.assertDictEqual(self.images_counts, res_image_counts)
            self.assertDictEqual(self.journal_counts, res_journal_counts)


if __name__ == '__main__':
    unittest.main()
