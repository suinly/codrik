#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BuiltinSkillFile {
    pub path: &'static str,
    pub content: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BuiltinSkill {
    pub name: &'static str,
    pub description: &'static str,
    pub files: &'static [BuiltinSkillFile],
}

const SKILL_CREATOR_FILES: &[BuiltinSkillFile] = &[BuiltinSkillFile {
    path: "SKILL.md",
    content: include_str!("../../.codrik/builtin-skills/skill-creator/SKILL.md"),
}];

pub(super) const SKILLS: &[BuiltinSkill] = &[BuiltinSkill {
    name: "skill-creator",
    description: "Use when creating, writing, saving, or updating reusable skills.",
    files: SKILL_CREATOR_FILES,
}];
