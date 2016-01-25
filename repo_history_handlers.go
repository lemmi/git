package git

import (
	"regexp"
)

func commitRootComparator(current, parent *Commit) bool {
	return current.TreeId().Equal(parent.TreeId())
}

func nopComparator(current, parent *Commit) bool {
	return false
}

func makePathComparator(path string) CommitComparator {
	return func(current, parent *Commit) bool {
		centry, cerr := current.GetTreeEntryByPath(path)
		pentry, perr := parent.GetTreeEntryByPath(path)

		if cerr != nil || perr != nil {
			return cerr == ErrNotExist && perr == ErrNotExist
		}

		return centry.Id.Equal(pentry.Id)
	}
}

func makePathChecker(path string) (cb CommitWalkCallback) {
	return func(commit *Commit) (HistoryWalkerAction, error) {
		_, err := commit.GetTreeEntryByPath(path)
		if err != nil {
			if err == ErrNotExist {
				return HWFollowParents, nil
			} else {
				return HWStop, err
			}
		}

		return HWTakeAndFollow, nil
	}
}

func makeHistorySearcher(needle string) (cb CommitWalkCallback, err error) {
	matcher, err := regexp.Compile(needle)
	if err != nil {
		return nil, err
	}

	return func(commit *Commit) (HistoryWalkerAction, error) {
		if matcher.MatchString(commit.CommitMessage) {
			return HWTakeAndFollow, nil
		}

		return HWFollowParents, nil
	}, nil
}

func nopCallback(*Commit) (HistoryWalkerAction, error) {
	return HWTakeAndFollow, nil
}

func makePager(cb CommitWalkCallback, skip int, count int) CommitWalkCallback {
	if cb == nil {
		cb = nopCallback
	}

	pagerCallback := func(commit *Commit) (HistoryWalkerAction, error) {
		pagerAction, err := cb(commit)
		if err != nil {
			return pagerAction, err
		}

		// if checker does not want to pick this commit, pager does not want either
		if pagerAction&HWTakeCommit == 0 {
			return pagerAction, nil
		}

		if skip != 0 {
			skip--
			action := pagerAction &^ HWTakeCommit
			return action, nil
		}

		if count != 0 {
			count--
			// this is last element we want to take
			if count == 0 {
				pagerAction |= HWStop
			}
			return pagerAction, nil
		}

		return HWStop, nil
	}

	return pagerCallback
}

func makeCounter(cb CommitWalkCallback) (CommitWalkCallback, func() int) {
	count := 0

	if cb == nil {
		cb = nopCallback
	}

	callback := func(commit *Commit) (HistoryWalkerAction, error) {
		action, err := cb(commit)
		if err != nil {
			return action, err
		}

		if action&HWTakeCommit > 0 {
			count++
			action = action &^ HWTakeCommit
		}

		return action, nil
	}

	getter := func() int {
		return count
	}

	return callback, getter
}
