import pandas as pd
import spacy
import duckdb
from spacy.matcher import PhraseMatcher
from skillNer.general_params import SKILL_DB
from skillNer.skill_extractor_class import SkillExtractor
from langdetect import detect

def should_remove(skill_name: str, desc: str = "") -> bool:
    BLACKLIST = {
        "e (programming language)", "library for www in perl",
        "component object model (com)", "hostile work environment", "sage safe x3",
        "inquiry", "workflows", "flooring", "target 3001!"
    }
    s = skill_name.lower().strip()
    if s in BLACKLIST:
        return True
    if s in {"r", "c"}:
        context = desc.lower()
        if s == "r" and ("r programming" in context or "rstudio" in context):
            return False
        if s == "c" and ("c programming" in context or "embedded c" in context or "c++" in context):
            return False
        return True
    if len(s) <= 2:
        return True
    return False

def main_enrichment(df: pd.DataFrame):
    nlp = spacy.load("en_core_web_lg")
    skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)

    seed_triggers = [
        "proficient", "experience", "skills", "required", "knowledge", "expertise", "purpose",
        "ability", "qualifications", "role", "responsible", "duties", "looking", "seeking",
        "task", "candidate", "ability"
    ]
    seed_docs = [nlp(trigger) for trigger in seed_triggers]
    similarity_threshold = 0.75

    def has_trigger(sentence_doc):
        for token in sentence_doc:
            if not token.has_vector:
                continue
            for seed in seed_docs:
                if token.similarity(seed) >= similarity_threshold:
                    return True
        return False

    def is_task_sentence(sentence_doc):
        for token in sentence_doc[:2]:
            if token.pos_ == "VERB" and (token.dep_ in {"ROOT", "conj"}):
                return True
            if token.pos_ == "VERB" and token.tag_ == "VBG":
                return True
        return False

    def is_english(text):
        try:
            return detect(text) == 'en'
        except:
            return False

    df = df[df["job description"].notna()]
    df = df[df["job description"].apply(is_english)]

    df['hard_skills'] = ""
    df['soft_skills'] = ""

    for idx, row in df.iterrows():
        job_description = row["job description"]
        doc = nlp(job_description)
        sentences = list(doc.sents)

        full_matches, ngram_scored, soft_skills, hard_skills = [], [], [], []

        for sent in sentences:
            if has_trigger(sent) or is_task_sentence(sent):
                annotations = skill_extractor.annotate(sent.text)
                for data in annotations['results']['full_matches']:
                    skill_name = SKILL_DB[data['skill_id']]['skill_name']
                    if not should_remove(skill_name, job_description):
                        full_matches.append(data['skill_id'])
                for data in annotations['results']['ngram_scored']:
                    skill_name = SKILL_DB[data['skill_id']]['skill_name']
                    if not should_remove(skill_name, job_description):
                        ngram_scored.append(data['skill_id'])

        for skill_id in set(full_matches + ngram_scored):
            skill_type = SKILL_DB[skill_id]['skill_type']
            skill_name = SKILL_DB[skill_id]['skill_name']
            if skill_type == 'Soft Skill':
                soft_skills.append(skill_name)
            elif skill_type == 'Hard Skill':
                hard_skills.append(skill_name)

        df.at[idx, 'hard_skills'] = ", ".join(hard_skills)
        df.at[idx, 'soft_skills'] = ", ".join(soft_skills)

    conn = duckdb.connect('./job_postings.duckdb')
    conn.register('raw_df', df)

    table_exists = conn.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' 
        AND table_name = 'raw_job_postings'
    """).fetchone()[0] > 0

    if table_exists:
        conn.execute("""
            DELETE FROM raw_job_postings
            WHERE job_link IN (SELECT job_link FROM raw_df)
        """)
        conn.execute("""
            INSERT INTO raw_job_postings
            SELECT * FROM raw_df
        """)
    else:
        conn.execute("CREATE TABLE raw_job_postings AS SELECT * FROM raw_df")

    conn.close()