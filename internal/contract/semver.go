package contract

import (
	"fmt"
	"strconv"
	"strings"
)

// semver 是解析后的语义化版本（major.minor.patch），仅处理数字核心版本。
// 非数字的预发布后缀按词法附加比较；无法解析的版本退化为 0.0.0。
type semver struct {
	major, minor, patch int
	pre                 string
	raw                 string
}

func parseSemver(v string) semver {
	s := semver{raw: v, pre: ""}
	core := v
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		core = v[:i]
		if v[i] == '-' {
			rest := v[i+1:]
			if j := strings.IndexByte(rest, '+'); j >= 0 {
				rest = rest[:j]
			}
			s.pre = rest
		}
	}
	parts := strings.Split(core, ".")
	nums := []*int{&s.major, &s.minor, &s.patch}
	for i := 0; i < len(parts) && i < len(nums); i++ {
		n, err := strconv.Atoi(strings.TrimSpace(parts[i]))
		if err == nil && n >= 0 {
			*nums[i] = n
		}
	}
	return s
}

// compare 返回 -1/0/1，表示 s 相对 t 的高低。
// 预发布规则：无预发布 > 有预发布；均有则按点分段词法/数值比较。
func (s semver) compare(t semver) int {
	if c := intCmp(s.major, t.major); c != 0 {
		return c
	}
	if c := intCmp(s.minor, t.minor); c != 0 {
		return c
	}
	if c := intCmp(s.patch, t.patch); c != 0 {
		return c
	}
	return comparePre(s.pre, t.pre)
}

func (s semver) String() string {
	if s.pre != "" {
		return fmt.Sprintf("%d.%d.%d-%s", s.major, s.minor, s.patch, s.pre)
	}
	return fmt.Sprintf("%d.%d.%d", s.major, s.minor, s.patch)
}

func intCmp(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// comparePre 实现语义化版本预发布标识比较。
func comparePre(a, b string) int {
	switch {
	case a == b:
		return 0
	case a == "": // 正式版高于预发布版
		return 1
	case b == "":
		return -1
	}
	as := strings.Split(a, ".")
	bs := strings.Split(b, ".")
	for i := 0; i < len(as) && i < len(bs); i++ {
		x, y := as[i], bs[i]
		xn, xerr := strconv.Atoi(x)
		yn, yerr := strconv.Atoi(y)
		switch {
		case xerr == nil && yerr == nil:
			if c := intCmp(xn, yn); c != 0 {
				return c
			}
		case xerr == nil && yerr != nil: // 数字段优先级低于字母数字段
			return -1
		case xerr != nil && yerr == nil:
			return 1
		default:
			if c := strings.Compare(x, y); c != 0 {
				return c
			}
		}
	}
	return intCmp(len(as), len(bs))
}

// CompareVersions 按语义化版本比较两个版本字符串，返回 -1/0/1。
func CompareVersions(a, b string) int {
	return parseSemver(a).compare(parseSemver(b))
}
