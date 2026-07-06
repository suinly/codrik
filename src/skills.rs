use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Skill {
    pub name: String,
    pub description: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRegistry {
    roots: Vec<SkillRoot>,
}

impl SkillRegistry {
    pub fn new(roots: Vec<SkillRoot>) -> Self {
        Self { roots }
    }

    pub fn list(&self) -> Result<Vec<Skill>> {
        Ok(self
            .discover()?
            .into_iter()
            .map(|entry| entry.skill)
            .collect())
    }

    pub fn read(&self, name: &str, relative_path: Option<&str>) -> Result<String> {
        let skill_dir = self
            .discover()?
            .into_iter()
            .find(|entry| entry.skill.name == name)
            .map(|entry| entry.dir)
            .with_context(|| format!("unknown skill: {name}"))?;
        let relative_path = relative_path.unwrap_or("SKILL.md");
        let path = resolve_inside(&skill_dir, relative_path)?;

        fs::read_to_string(&path)
            .with_context(|| format!("failed to read skill file: {}", path.display()))
    }

    fn discover(&self) -> Result<Vec<DiscoveredSkill>> {
        let mut skills = Vec::new();
        let mut seen_names = BTreeSet::new();

        for root in &self.roots {
            if !root.path.is_dir() {
                continue;
            }

            let mut entries = fs::read_dir(&root.path)
                .with_context(|| format!("failed to read skills root: {}", root.path.display()))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            entries.sort_by_key(|entry| entry.file_name());

            for entry in entries {
                if !entry.file_type()?.is_dir() {
                    continue;
                }

                let skill_dir = entry.path();
                let skill_md = skill_dir.join("SKILL.md");
                if !skill_md.is_file() {
                    continue;
                }

                let fallback_name = entry.file_name().to_string_lossy().to_string();
                let content = fs::read_to_string(&skill_md)
                    .with_context(|| format!("failed to read skill: {}", skill_md.display()))?;
                let metadata = parse_metadata(&content);
                let name = metadata.name.unwrap_or(fallback_name);
                if !seen_names.insert(name.clone()) {
                    continue;
                }

                skills.push(DiscoveredSkill {
                    dir: skill_dir,
                    skill: Skill {
                        name,
                        description: metadata.description.unwrap_or_default(),
                        source: root.source.clone(),
                    },
                });
            }
        }

        Ok(skills)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct DiscoveredSkill {
    dir: PathBuf,
    skill: Skill,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillRoot {
    path: PathBuf,
    source: String,
    writable: bool,
}

impl SkillRoot {
    pub fn read_only(path: impl Into<PathBuf>, source: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            source: source.into(),
            writable: false,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
struct SkillMetadata {
    name: Option<String>,
    description: Option<String>,
}

fn parse_metadata(content: &str) -> SkillMetadata {
    let Some(rest) = content.strip_prefix("---\n") else {
        return SkillMetadata::default();
    };
    let Some((frontmatter, _body)) = rest.split_once("\n---") else {
        return SkillMetadata::default();
    };

    yaml_serde::from_str(frontmatter).unwrap_or_default()
}

fn resolve_inside(root: &Path, relative_path: &str) -> Result<PathBuf> {
    let relative = Path::new(relative_path);
    if relative.is_absolute() {
        bail!("skill path must be relative");
    }
    if relative
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("skill path escapes skill directory");
    }

    let root = root
        .canonicalize()
        .with_context(|| format!("failed to canonicalize skill directory: {}", root.display()))?;
    let target = root.join(relative);
    let target = target
        .canonicalize()
        .with_context(|| format!("failed to canonicalize skill file: {}", target.display()))?;
    if !target.starts_with(&root) {
        bail!("skill path escapes skill directory");
    }

    Ok(target)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::Result;

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn list_discovers_skill_frontmatter() -> Result<()> {
        let root = temp_root("list-discovers")?;
        write_skill(
            &root,
            "telegram-debug",
            "---\nname: telegram-debug\ndescription: Use when debugging Telegram gateway failures.\n---\n# Telegram Debug\n",
        )?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let skills = registry.list()?;

        assert_eq!(
            skills,
            vec![Skill {
                name: "telegram-debug".to_string(),
                description: "Use when debugging Telegram gateway failures.".to_string(),
                source: "test".to_string(),
            }]
        );
        Ok(())
    }

    #[test]
    fn list_parses_folded_multiline_description() -> Result<()> {
        let root = temp_root("list-folded-description")?;
        write_skill(
            &root,
            "telegram-debug",
            "---\nname: telegram-debug\ndescription: >\n  Use when debugging\n  Telegram gateway failures.\n---\n# Telegram Debug\n",
        )?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let skills = registry.list()?;

        assert!(skills[0].description.contains("Use when debugging"));
        assert!(skills[0].description.contains("Telegram gateway failures."));
        Ok(())
    }

    #[test]
    fn list_uses_directory_name_when_name_is_missing() -> Result<()> {
        let root = temp_root("list-dir-name")?;
        write_skill(
            &root,
            "release",
            "---\ndescription: Release checklist.\n---\n# Release\n",
        )?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let skills = registry.list()?;

        assert_eq!(skills[0].name, "release");
        assert_eq!(skills[0].description, "Release checklist.");
        Ok(())
    }

    #[test]
    fn list_deduplicates_by_root_precedence() -> Result<()> {
        let project = temp_root("project")?;
        let user = temp_root("user")?;
        write_skill(
            &project,
            "tdd",
            "---\ndescription: Project TDD.\n---\n# TDD\n",
        )?;
        write_skill(&user, "tdd", "---\ndescription: User TDD.\n---\n# TDD\n")?;
        let registry = SkillRegistry::new(vec![
            SkillRoot::read_only(&project, "project"),
            SkillRoot::read_only(&user, "user"),
        ]);

        let skills = registry.list()?;

        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].description, "Project TDD.");
        assert_eq!(skills[0].source, "project");
        Ok(())
    }

    #[test]
    fn list_preserves_root_order_before_global_name_order() -> Result<()> {
        let project = temp_root("project-order")?;
        let user = temp_root("user-order")?;
        write_skill(&project, "z-project", "# Project Skill\n")?;
        write_skill(&user, "a-user", "# User Skill\n")?;
        let registry = SkillRegistry::new(vec![
            SkillRoot::read_only(&project, "project"),
            SkillRoot::read_only(&user, "user"),
        ]);

        let skills = registry.list()?;

        assert_eq!(
            skills
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            vec!["z-project", "a-user"]
        );
        Ok(())
    }

    #[test]
    fn read_skill_md_returns_content() -> Result<()> {
        let root = temp_root("read-skill")?;
        write_skill(&root, "tdd", "# TDD\nUse tests first.\n")?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let content = registry.read("tdd", None)?;

        assert_eq!(content, "# TDD\nUse tests first.\n");
        Ok(())
    }

    #[test]
    fn read_uses_frontmatter_name_from_discovery() -> Result<()> {
        let root = temp_root("read-frontmatter-name")?;
        write_skill(
            &root,
            "directory-name",
            "---\nname: public-name\ndescription: Public skill.\n---\n# Public Skill\n",
        )?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let content = registry.read("public-name", None)?;

        assert!(content.contains("# Public Skill"));
        Ok(())
    }

    #[test]
    fn read_relative_reference_stays_inside_skill_directory() -> Result<()> {
        let root = temp_root("read-ref")?;
        write_skill(&root, "tdd", "# TDD\n")?;
        fs::create_dir_all(root.join("tdd").join("references"))?;
        fs::write(
            root.join("tdd").join("references").join("loop.md"),
            "red green\n",
        )?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let content = registry.read("tdd", Some("references/loop.md"))?;

        assert_eq!(content, "red green\n");
        Ok(())
    }

    #[test]
    fn read_rejects_path_traversal() -> Result<()> {
        let root = temp_root("reject-traversal")?;
        write_skill(&root, "tdd", "# TDD\n")?;
        fs::write(root.join("secret.md"), "secret")?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let error = registry.read("tdd", Some("../secret.md")).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("skill path escapes skill directory")
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn read_rejects_symlink_that_escapes_skill_directory() -> Result<()> {
        use std::os::unix::fs::symlink;

        let root = temp_root("reject-symlink")?;
        write_skill(&root, "tdd", "# TDD\n")?;
        fs::create_dir_all(root.join("tdd").join("references"))?;
        fs::write(root.join("secret.md"), "secret")?;
        symlink(
            root.join("secret.md"),
            root.join("tdd").join("references").join("link.md"),
        )?;
        let registry = SkillRegistry::new(vec![SkillRoot::read_only(&root, "test")]);

        let error = registry
            .read("tdd", Some("references/link.md"))
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("skill path escapes skill directory")
        );
        Ok(())
    }

    fn write_skill(root: &Path, name: &str, content: &str) -> Result<()> {
        let dir = root.join(name);
        fs::create_dir_all(&dir)?;
        fs::write(dir.join("SKILL.md"), content)?;
        Ok(())
    }

    fn temp_root(label: &str) -> Result<PathBuf> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos()
            .to_string();
        let path = std::env::temp_dir().join(format!(
            "codrik-skills-{label}-{}-{unique}",
            TEMP_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&path)?;
        Ok(path)
    }
}
