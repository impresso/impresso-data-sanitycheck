#!/usr/bin/env python
# coding: utf-8 
# created on 2018.03.31 using PyCharm 
# project impresso-image-acquisition

from unittest import TestCase
import unittest

from images import check_images
from impresso_commons import path

__author__ = "maudehrmann"


class TestRunCheckOriginal(TestCase):
    global orig_dir
    global newspapers
    orig_dir = "../../test-data/original"
    newspapers = "ORIGTEST"

    original_cases = {'issues w/o zip': ['ORIGTEST/1930/06/11'],
                      'issues w/ corruptedzip': ['ORIGTEST/1881/02/26'],
                      'issues_homogeneouscoverage_tifs': ['ORIGTEST/1900/01/10'],
                      'issues_homogeneouscoverage_pngs': ['ORIGTEST/1900/01/11'],
                      'issues_homogeneouscoverage_jpgs': ['ORIGTEST/1900/01/12'],
                      'issues_homogeneouscoverage_singlepngs': [],
                      'issues_heterocoverage_all': [],
                      'issues_heterocoverage_tif_png': [],
                      'issues_heterocoverage_tif_jpg': [],
                      'issues_heterocoverage_png_jpg': [],
                      'issues_missing_pageimg': []}

    journal_counts = {'number original issues': 7,
                      'number valid original issues': 5,
                      'issues w/o large pdf': 0,
                      'issues w/o small pdfs': 0,
                      'issues w/o both pdfs': 0,
                      'issues w both pdfs': 5,
                      'number of pages': 28}

    def test_run_check_original(self):
        journals = newspapers.split(" ")

        for journal in journals:
            # detect issues to consider for  journal
            original_issues = path.detect_journal_issues(orig_dir, journal)

            # check
            res_journal_original_cases, res_journal_counts = check_images.check_original_journal(original_issues, None)

            self.assertDictEqual(self.original_cases, res_journal_original_cases)
            self.assertDictEqual(self.journal_counts, res_journal_counts)


if __name__ == '__main__':
    unittest.main()
