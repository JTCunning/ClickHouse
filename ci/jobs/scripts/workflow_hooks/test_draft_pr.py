from types import SimpleNamespace

import pytest

from ci.defs.defs import JobNames
from ci.jobs.scripts.workflow_hooks import filter_job
from ci.jobs.scripts.workflow_hooks.can_be_merged import is_draft_only_run
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels


def _info(labels=(), is_draft=True, workflow_name="PR"):
    return SimpleNamespace(
        pr_labels=set(labels),
        pr_is_draft=is_draft,
        workflow_name=workflow_name,
        pr_number=1,
        pr_title="Some change",
        sha="0" * 40,
        get_kv_data=lambda key: None,
        add_workflow_note=lambda note: None,
    )


@pytest.fixture
def info_for(monkeypatch):
    monkeypatch.setattr(filter_job, "_is_empty_merge_commit", lambda sha: False)

    def set_info(**kwargs):
        monkeypatch.setattr(filter_job, "_info_cache", _info(**kwargs))

    return set_info


def test_draft_skips_review_without_label(info_for):
    info_for()
    skip, reason = filter_job.should_skip_job(JobNames.CODE_REVIEW)
    assert skip
    assert reason == filter_job.DRAFT_PR_SKIP_REASON


def test_draft_with_label_runs_review(info_for):
    info_for(labels={Labels.CI_CODE_REVIEW})
    skip, reason = filter_job.should_skip_job(JobNames.CODE_REVIEW)
    assert not skip, reason


def test_draft_with_label_still_skips_other_jobs(info_for):
    info_for(labels={Labels.CI_CODE_REVIEW})
    skip, reason = filter_job.should_skip_job(JobNames.STRESS)
    assert skip
    assert reason == filter_job.DRAFT_PR_SKIP_REASON


def test_draft_runs_fast_test(info_for):
    info_for()
    skip, _ = filter_job.should_skip_job(JobNames.FAST_TEST)
    assert not skip


def test_ready_pr_is_not_skipped_by_draft_rule(info_for):
    info_for(is_draft=False)
    skip, reason = filter_job.should_skip_job(JobNames.CODE_REVIEW)
    assert not skip, reason


def test_backport_draft_is_not_skipped_by_draft_rule(info_for):
    info_for(workflow_name="BackportPR")
    skip, reason = filter_job.should_skip_job(JobNames.CODE_REVIEW)
    assert not skip, reason


def test_merge_gate_fails_for_draft_with_review_label():
    assert is_draft_only_run(_info(labels={Labels.CI_CODE_REVIEW}))


def test_merge_gate_fails_for_draft_without_label():
    assert is_draft_only_run(_info())


def test_merge_gate_passes_with_force_all_label():
    assert not is_draft_only_run(
        _info(labels={Labels.CI_CODE_REVIEW, Labels.CI_FORCE_ALL})
    )


def test_merge_gate_ignores_ready_pr():
    assert not is_draft_only_run(_info(labels={Labels.CI_CODE_REVIEW}, is_draft=False))
