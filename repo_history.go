package git

import (
	"container/list"
)

type HistoryWalkerAction int

const (
	// drop commit and do not follow parents
	HWDrop HistoryWalkerAction = 0
	// take commit but not traverse parents
	HWTakeCommit HistoryWalkerAction = 1 << iota
	// drop commit but traverse parents
	HWFollowParents
	// stop traverse
	HWStop

	// take commit and follow parents
	HWTakeAndFollow = HWTakeCommit | HWFollowParents
)

type CommitWalkCallback func(*Commit) (HistoryWalkerAction, error)

// CommitComparator defines callback type for checking commit equalty. If it returns true then
// commits are considered equal. See "History Simplification" chapter of git-log man for details
type CommitComparator func(current, parent *Commit) bool

func walkHistory(start *Commit, callback CommitWalkCallback) (*list.List, error) {
	return walkHistoryLoop([]*Commit{start}, callback, commitRootComparator)
}

func walkFilteredHistory(start *Commit, callback CommitWalkCallback,
	eq CommitComparator) (*list.List, error) {

	return walkHistoryLoop([]*Commit{start}, callback, eq)
}

// roots must be not equal to each other
func walkHistoryLoop(roots []*Commit, callback CommitWalkCallback,
	eq CommitComparator) (*list.List, error) {

	results := list.New()
	seen := make(map[sha1]struct{})

	for {
		if len(roots) == 0 {
			return results, nil
		}

		var err error

		roots, err = simplifyRoots(roots, eq, seen)
		if err != nil {
			return nil, err
		}

		var next *Commit
		next, roots = extractNewestCommit(roots)

		action, err := callback(next)
		if err != nil {
			return nil, err
		}

		if action&HWTakeCommit > 0 {
			// witness commit
			results.PushBack(next)
			seen[next.Id] = struct{}{}
		}

		if action&HWFollowParents > 0 {
			// follow all parents of commit
			pars, err := parents(next)
			if err != nil {
				return nil, err
			}
			roots = mergeRoots(pars, roots, eq, seen)
		}

		if action&HWStop > 0 {
			return results, nil
		}
	}

	return results, nil
}

func parents(commit *Commit) ([]*Commit, error) {
	parents := make([]*Commit, commit.ParentCount())
	for idx := 0; idx < len(parents); idx++ {
		var err error
		parents[idx], err = commit.Parent(idx)
		if err != nil {
			return nil, err
		}
	}
	return parents, nil
}

// mergeRoots will merge two sets of commits and ensure that they are not equal to each other
// the members of base and merging sets already nonequal to each other
func mergeRoots(base, merging []*Commit, eq CommitComparator, seen map[sha1]struct{}) []*Commit {
	newRoots := append([]*Commit(nil), base...)
	for _, needle := range merging {
		found := false
		for _, item := range base {
			if eq(needle, item) {
				// found equal commit in merging roots
				// drop it and mark as seen
				seen[needle.Id] = struct{}{}
				found = true
				break
			}
		}

		if !found {
			// commit is unique enough
			newRoots = append(newRoots, needle)
		}
	}

	return newRoots
}

// skipEqualCommits compares commit to it parents. If it finds a parent
// that equals to current commit the current commit will be dropped and parent will be followed
// see "History Simplification" chapter of git-log man for full details.
func skipEqualCommits(commit *Commit, eq CommitComparator,
	seen map[sha1]struct{}) (*Commit, error) {

	for {
		// we already seen that commit, no point to traverse further
		if _, ok := seen[commit.Id]; ok {
			return nil, nil
		}

		if len(commit.parents) == 0 {
			return commit, nil
		}

		var found bool
		for idx := 0; idx < commit.ParentCount(); idx++ {
			parent, err := commit.Parent(idx)
			if err != nil {
				return nil, err
			}

			if eq(commit, parent) {
				// we have parent that equals to given commit
				// so take this parent as next commit (dropping current)
				// but we will remember that we seen current commit in history
				seen[commit.Id] = struct{}{}
				commit = parent
				found = true
				break
			}
		}

		// all parents of commit was not same to it, so return this commit
		if !found {
			return commit, nil
		}
	}
}

func simplifyRoots(roots []*Commit, eq CommitComparator,
	seen map[sha1]struct{}) ([]*Commit, error) {

	newRoots := []*Commit{}
	for _, commit := range roots {
		commit, err := skipEqualCommits(commit, eq, seen)
		if err != nil {
			return nil, err
		}
		if commit != nil {
			newRoots = append(newRoots, commit)
		}
	}

	return newRoots, nil
}

// extractNewestCommit will find newest commit, extract it and return resulting set
func extractNewestCommit(roots []*Commit) (*Commit, []*Commit) {
	if len(roots) == 1 {
		return roots[0], roots[:0]
	}

	target := roots[0]
	targetIdx := 0
	for idx, current := range roots[1:] {
		if current.Committer.When.After(target.Committer.When) {
			target = current
			targetIdx = idx
		}
	}

	// remove picked commit
	roots = append(roots[:targetIdx], roots[targetIdx+1:]...)

	return target, roots
}
