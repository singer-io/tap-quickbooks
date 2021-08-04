import unittest
import tap_quickbooks.streams as streams


class TestReportsParser(unittest.TestCase):

    def test_day_wise_reports(self):
        """
        Test that day wise reports are generated from formatted parsed_metadata
        """
        reports = streams.ReportStream("", "", "")
        reports.parsed_metadata = {
            "dates": ["2021-07-01", "2021-07-02", "2021-07-03"],
            "data": [
                {
                    "name": "Total Income",
                    "values": ["4.00", "5.00", "6.00", "15.00"]
                }, {
                    "name": "Gross Profit",
                    "values": ["3.00", "4.00", "5.00", "12.00"]
                }
            ]
        }

        expected_records = [
            {
                "ReportDate": "2021-07-01",
                "AccountingMethod": "Accrual",
                "Details": {"Total Income": "4.00", "Gross Profit": "3.00"}
            },
            {
                "ReportDate": "2021-07-02",
                "AccountingMethod": "Accrual",
                "Details": {"Total Income": "5.00", "Gross Profit": "4.00"}
            },
            {
                "ReportDate": "2021-07-03",
                "AccountingMethod": "Accrual",
                "Details": {"Total Income": "6.00", "Gross Profit": "5.00"}
            }
        ]

        records = list(reports.day_wise_reports())
        self.assertListEqual(expected_records, records)

    def test_report_parser(self):
        """
        Test that metadata returned from report API is formatted in desired format
        """
        reports = streams.ReportStream("", "", "")

        response = {
            "Header": {
                "Time": "2021-07-22T05:51:37-07:00",
                "ReportName": "ProfitAndLoss",
                "ReportBasis": "Accrual",
                "StartPeriod": "2021-07-20",
                "EndPeriod": "2021-07-21",
                "SummarizeColumnsBy": "Days",
                "Currency": "USD",
                "Option": [{
                    "Name": "AccountingStandard",
                    "Value": "GAAP"
                }, {
                    "Name": "NoReportData",
                    "Value": "true"
                }]
            },
            "Columns": {
                "Column": [{
                    "ColTitle": "",
                    "ColType": "Account",
                    "MetaData": [{
                        "Name": "ColKey",
                        "Value": "account"
                    }]
                }, {
                    "ColTitle": "Jul 20, 2021",
                    "ColType": "Money",
                    "MetaData": [{
                        "Name": "StartDate",
                        "Value": "2021-07-20"
                    }, {
                        "Name": "EndDate",
                        "Value": "2021-07-20"
                    }, {
                        "Name": "ColKey",
                        "Value": "Jul 20, 2021"
                    }]
                }, {
                    "ColTitle": "Jul 21, 2021",
                    "ColType": "Money",
                    "MetaData": [{
                        "Name": "StartDate",
                        "Value": "2021-07-21"
                    }, {
                        "Name": "EndDate",
                        "Value": "2021-07-21"
                    }, {
                        "Name": "ColKey",
                        "Value": "Jul 21, 2021"
                    }]
                }, {
                    "ColTitle": "Total",
                    "ColType": "Money",
                    "MetaData": [{
                        "Name": "ColKey",
                        "Value": "total"
                    }]
                }]
            },
            "Rows": {
                "Row": [{
                    "Header": {
                        "ColData": [{
                            "value": "Income"
                        }, {
                            "value": ""
                        }, {
                            "value": ""
                        }, {
                            "value": ""
                        }]
                    },
                    "Summary": {
                        "ColData": [{
                            "value": "Total Income"
                        }, {
                            "value": ""
                        }, {
                            "value": ""
                        }, {
                            "value": "0.00"
                        }]
                    },
                    "type": "Section",
                    "group": "Income"
                }, {
                    "Summary": {
                        "ColData": [{
                            "value": "Gross Profit"
                        }, {
                            "value": "0.00"
                        }, {
                            "value": "0.00"
                        }, {
                            "value": "0.00"
                        }]
                    },
                    "type": "Section",
                    "group": "GrossProfit"
                }, {
                    "Header": {
                        "ColData": [{
                            "value": "Expenses"
                        }, {
                            "value": ""
                        }, {
                            "value": ""
                        }, {
                            "value": ""
                        }]
                    },
                    "Summary": {
                        "ColData": [{
                            "value": "Total Expenses"
                        }, {
                            "value": ""
                        }, {
                            "value": ""
                        }, {
                            "value": "0.00"
                        }]
                    },
                    "type": "Section",
                    "group": "Expenses"
                }, {
                    "Summary": {
                        "ColData": [{
                            "value": "Net Operating Income"
                        }, {
                            "value": "0.00"
                        }, {
                            "value": "0.00"
                        }, {
                            "value": "0.00"
                        }]
                    },
                    "type": "Section",
                    "group": "NetOperatingIncome"
                }, {
                    "Summary": {
                        "ColData": [{
                            "value": "Net Income"
                        }, {
                            "value": "0.00"
                        }, {
                            "value": "0.00"
                        }, {
                            "value": "0.00"
                        }]
                    },
                    "type": "Section",
                    "group": "NetIncome"
                }]
            }
        }

        expected_data = {
            "dates": ["2021-07-20", "2021-07-21"],
            "data": [{
                "name": "Total Income",
                "values": ["", "", "0.00"]
            }, {
                "name": "Gross Profit",
                "values": ["0.00", "0.00", "0.00"]
            }, {
                "name": "Total Expenses",
                "values": ["", "", "0.00"]
            }, {
                "name": "Net Operating Income",
                "values": ["0.00", "0.00", "0.00"]
            }, {
                "name": "Net Income",
                "values": ["0.00", "0.00", "0.00"]
            }]
        }

        reports.parse_report_metadata(response)
        self.assertEqual(reports.parsed_metadata, expected_data)
    
