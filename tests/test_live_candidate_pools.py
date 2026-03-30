from datetime import date
import unittest
from unittest.mock import patch

from jczq_assistant import sfc500_history
from jczq_assistant.zgzcw_live import parse_zgzcw_issue_options
from jczq_assistant.zgzcw_live import parse_zgzcw_issue_page


class LiveCandidatePoolsTest(unittest.TestCase):
    def test_fetch_sale_issue_matches_snapshot_includes_current_displayed_expect(self):
        landing_html = """
        <html>
          <body>
            <div>官方在售第26051期</div>
            <div class="subnav-sshc">
              <ul>
                <li><a data-expect="26051">第26051期</a></li>
                <li class="on"><a data-expect="26050">第26050期</a></li>
              </ul>
            </div>
            <table id="vsTable">
              <tr
                class="bet-tb-tr"
                data-bjpl="2.78,3.19,2.43"
                data-pjgl="0.33,0.31,0.36"
                data-asian="0.95,-0.5,0.91"
                data-kl="0.92,0.91,0.93"
              >
                <td class="td td-no">1</td>
                <td class="td td-evt"><a>世预赛</a></td>
                <td class="td td-endtime">03-30 19:00</td>
                <td class="td td-team">
                  <div class="team">
                    <span class="team-l"><a>主队A</a></span>
                    <i class="team-vs">VS</i>
                    <span class="team-r"><a>客队B</a></span>
                  </div>
                </td>
                <td class="td td-betbtn"><div class="betbtn-row"></div></td>
                <td class="td td-data"><a href="https://example.com/a">析</a></td>
              </tr>
            </table>
          </body>
        </html>
        """
        expect_26051_rows = [
            {
                "match_no": 1,
                "competition": "世预赛",
                "match_time": "2026-04-01 00:00:00",
                "home_team": "主队C",
                "away_team": "客队D",
                "final_score": None,
                "is_settled": 0,
                "avg_win_odds": None,
                "avg_draw_odds": None,
                "avg_lose_odds": None,
                "analysis_url": "https://example.com/b",
                "asian_line": None,
            }
        ]

        with (
            patch.object(sfc500_history, "_fetch_sfc_index_html", return_value=landing_html),
            patch.object(
                sfc500_history,
                "fetch_issue_matches",
                side_effect=lambda expect, **_: expect_26051_rows if expect == "26051" else [],
            ),
        ):
            snapshot = sfc500_history.fetch_sale_issue_matches_snapshot(expect="26050")

        self.assertEqual(snapshot["selected_issue"], "26050")
        self.assertEqual([option["issue"] for option in snapshot["issue_options"]], ["26051", "26050"])
        self.assertEqual(snapshot["issue_options"][0]["label"], "第26051期（官方在售）")
        self.assertEqual(snapshot["issue_options"][1]["label"], "第26050期（页面当前显示）")
        self.assertEqual(snapshot["matches"][0]["avg_win_odds"], 2.78)
        self.assertEqual(snapshot["matches"][0]["asian_handicap_line"], "-0.5")

    def test_parse_zgzcw_issue_options_keeps_future_issues(self):
        html = """
        <select id="selectissue">
          <option value="2026-03-28" selected="selected">2026-03-28</option>
          <option value="2026-03-27">2026-03-27</option>
          <option value="2026-03-26">2026-03-26</option>
        </select>
        """

        issue_options, default_issue = parse_zgzcw_issue_options(
            html,
            today=date(2026, 3, 27),
        )

        self.assertEqual([option["issue"] for option in issue_options], ["2026-03-28", "2026-03-27"])
        self.assertEqual(default_issue, "2026-03-28")
        self.assertEqual(issue_options[0]["label"], "2026-03-28（默认）")
        self.assertEqual(issue_options[1]["label"], "2026-03-27（今日）")

    def test_parse_zgzcw_issue_page_reads_open_sale_rows_only(self):
        html = """
        <table>
          <tr class="beginBet even" id="tr_2038664" m="日职联">
            <td class="wh-1"><a><i>001</i></a></td>
            <td class="wh-2"><a href="http://saishi.zgzcw.com/soccer/league/567">日职联</a></td>
            <td class="wh-3">
              <span title="截期时间:2026-03-28 12:40">12:40</span>
              <span title="比赛时间:2026-03-28 13:00">13:00</span>
            </td>
            <td class="wh-4 t-r">
              <a href="http://saishi.zgzcw.com/soccer/team/567/25413" title="町田泽维">町田泽维</a>
            </td>
            <td class="wh-5 bf">VS</td>
            <td class="wh-6 t-l">
              <a href="http://saishi.zgzcw.com/soccer/team/567/10908" title="川崎前锋">川崎前锋</a>
            </td>
            <td class="wh-8 b-l">
              <div class="tz-area frq" pid="49">
                <a class="weisai">1.79</a><a class="weisai">3.56</a><a class="weisai">3.45</a>
              </div>
              <div class="tz-area tz-area-2 rqq" pid="22">
                <em class="rq jian">-1</em>
              </div>
            </td>
            <td class="wh-10 b-l" newplayid="4470665">亚 欧 析</td>
          </tr>
          <tr class="endBet odd" m="英超">
            <td class="wh-1"><a><i>002</i></a></td>
          </tr>
        </table>
        """

        matches = parse_zgzcw_issue_page(html, "2026-03-28")

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["fixture_id"], 4470665)
        self.assertEqual(matches[0]["competition"], "日职联")
        self.assertEqual(matches[0]["match_time"], "2026-03-28 13:00:00")
        self.assertEqual(matches[0]["home_team_id"], 25413)
        self.assertEqual(matches[0]["away_team_id"], 10908)
        self.assertEqual(matches[0]["avg_win_odds"], 1.79)
        self.assertEqual(matches[0]["avg_draw_odds"], 3.56)
        self.assertEqual(matches[0]["avg_lose_odds"], 3.45)
        self.assertEqual(matches[0]["asian_handicap_line"], "-1")


if __name__ == "__main__":
    unittest.main()
