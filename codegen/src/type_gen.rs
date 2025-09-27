use marksman_escape::Escape;
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::iter;
use std::str;

use crate::snake_to_camel;

const PG_TYPE_DAT: &str = include_str!("pg_type.dat");
const PG_RANGE_DAT: &str = include_str!("pg_range.dat");

struct Type {
    name: String,
    variant: String,
    ident: String,
    kind: String,
    typtype: Option<String>,
    element: u32,
    doc: String,
}

pub fn build() {
    let mut file = BufWriter::new(File::create("../postgres-types/src/type_gen.rs").unwrap());
    let types = parse_types();

    make_header(&mut file);
    make_enum(&mut file, &types);
    make_impl(&mut file, &types);
    make_consts(&mut file, &types);
}

struct DatParser<'a> {
    it: iter::Peekable<str::CharIndices<'a>>,
    s: &'a str,
}

impl<'a> DatParser<'a> {
    fn new(s: &'a str) -> DatParser<'a> {
        DatParser {
            it: s.char_indices().peekable(),
            s,
        }
    }

    fn parse_array(&mut self) -> Vec<HashMap<String, String>> {
        self.eat('[');
        let mut vec = vec![];
        while !self.try_eat(']') {
            let object = self.parse_object();
            vec.push(object);
        }
        self.eof();

        vec
    }

    fn parse_object(&mut self) -> HashMap<String, String> {
        let mut object = HashMap::new();

        self.eat('{');
        loop {
            let key = self.parse_ident();
            self.eat('=');
            self.eat('>');
            let value = self.parse_string();
            object.insert(key, value);
            if !self.try_eat(',') {
                break;
            }
        }
        self.eat('}');
        self.eat(',');

        object
    }

    fn parse_ident(&mut self) -> String {
        self.skip_ws();

        let start = match self.it.peek() {
            Some((i, _)) => *i,
            None => return "".to_string(),
        };

        loop {
            match self.it.peek() {
                Some((_, 'a'..='z')) | Some((_, '_')) => {
                    self.it.next();
                }
                Some((i, _)) => return self.s[start..*i].to_string(),
                None => return self.s[start..].to_string(),
            }
        }
    }

    fn parse_string(&mut self) -> String {
        self.skip_ws();

        let mut s = String::new();

        self.eat('\'');
        loop {
            match self.it.next() {
                Some((_, '\'')) => return s,
                Some((_, '\\')) => {
                    let (_, ch) = self.it.next().expect("unexpected eof");
                    s.push(ch);
                }
                Some((_, ch)) => s.push(ch),
                None => panic!("unexpected eof"),
            }
        }
    }

    fn eat(&mut self, target: char) {
        self.skip_ws();

        match self.it.next() {
            Some((_, ch)) if ch == target => {}
            Some((_, ch)) => panic!("expected {} but got {}", target, ch),
            None => panic!("expected {} but got eof", target),
        }
    }

    fn try_eat(&mut self, target: char) -> bool {
        if self.peek(target) {
            self.eat(target);
            true
        } else {
            false
        }
    }

    fn peek(&mut self, target: char) -> bool {
        self.skip_ws();

        matches!(self.it.peek(), Some((_, ch)) if *ch == target)
    }

    fn eof(&mut self) {
        self.skip_ws();
        if let Some((_, ch)) = self.it.next() {
            panic!("expected eof but got {}", ch);
        }
    }

    fn skip_ws(&mut self) {
        loop {
            match self.it.peek() {
                Some(&(_, '#')) => self.skip_to('\n'),
                Some(&(_, '\n')) | Some(&(_, ' ')) | Some(&(_, '\t')) => {
                    self.it.next();
                }
                _ => break,
            }
        }
    }

    fn skip_to(&mut self, target: char) {
        for (_, ch) in &mut self.it {
            if ch == target {
                break;
            }
        }
    }
}

fn parse_types() -> BTreeMap<u32, Type> {
    let raw_types = DatParser::new(PG_TYPE_DAT).parse_array();
    let raw_ranges = DatParser::new(PG_RANGE_DAT).parse_array();

    let oids_by_name = raw_types
        .iter()
        .map(|m| (m["typname"].clone(), m["oid"].parse::<u32>().unwrap()))
        .collect::<HashMap<_, _>>();

    let range_elements = raw_ranges
        .iter()
        .map(|m| {
            (
                oids_by_name[&*m["rngtypid"]],
                oids_by_name[&*m["rngsubtype"]],
            )
        })
        .collect::<HashMap<_, _>>();
    let multi_range_elements = raw_ranges
        .iter()
        .map(|m| {
            (
                oids_by_name[&*m["rngmultitypid"]],
                oids_by_name[&*m["rngsubtype"]],
            )
        })
        .collect::<HashMap<_, _>>();

    let range_vector_re = Regex::new("(range|vector)$").unwrap();
    let array_re = Regex::new("^_(.*)").unwrap();

    let mut types = BTreeMap::new();

    for raw_type in raw_types {
        let oid = raw_type["oid"].parse::<u32>().unwrap();

        let name = raw_type["typname"].clone();

        let ident = range_vector_re.replace(&name, "_$1");
        let ident = array_re.replace(&ident, "${1}_array");
        let variant = snake_to_camel(&ident);
        let ident = ident.to_ascii_uppercase();

        let kind = raw_type["typcategory"].clone();

        // we need to be able to pull composite fields and enum variants at runtime
        if kind == "C" || kind == "E" {
            continue;
        }

        let typtype = raw_type.get("typtype").cloned();

        let element = match &*kind {
            "R" => match typtype
                .as_ref()
                .expect("range type must have typtype")
                .as_str()
            {
                "r" => range_elements[&oid],
                "m" => multi_range_elements[&oid],
                typtype => panic!("invalid range typtype {}", typtype),
            },
            "A" => oids_by_name[&raw_type["typelem"]],
            _ => 0,
        };

        let doc_name = array_re.replace(&name, "$1[]").to_ascii_uppercase();
        let mut doc = doc_name.clone();
        if let Some(descr) = raw_type.get("descr") {
            write!(doc, " - {descr}").unwrap();
        }
        let doc = Escape::new(doc.as_bytes().iter().cloned()).collect();
        let doc = String::from_utf8(doc).unwrap();

        if let Some(array_type_oid) = raw_type.get("array_type_oid") {
            let array_type_oid = array_type_oid.parse::<u32>().unwrap();

            let name = format!("_{name}");
            let variant = format!("{variant}Array");
            let doc = format!("{doc_name}&#91;&#93;");
            let ident = format!("{ident}_ARRAY");

            let type_ = Type {
                name,
                variant,
                ident,
                kind: "A".to_string(),
                typtype: None,
                element: oid,
                doc,
            };
            types.insert(array_type_oid, type_);
        }

        let type_ = Type {
            name,
            variant,
            ident,
            kind,
            typtype,
            element,
            doc,
        };
        types.insert(oid, type_);
    }

    types
}

fn make_header(w: &mut BufWriter<File>) {
    write!(
        w,
        "// Autogenerated file - DO NOT EDIT
use std::sync::Arc;

use crate::{{Type, Oid, Kind}};

#[derive(PartialEq, Eq, Debug, Hash)]
pub struct Other {{
    pub name: String,
    pub oid: Oid,
    pub kind: Kind,
    pub schema: String,
}}
"
    )
    .unwrap();
}

fn make_enum(w: &mut BufWriter<File>, types: &BTreeMap<u32, Type>) {
    write!(
        w,
        "
#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub enum Inner {{"
    )
    .unwrap();

    for type_ in types.values() {
        write!(
            w,
            "
    {},",
            type_.variant
        )
        .unwrap();
    }

    write!(
        w,
        r"
    Other(Arc<Other>),
}}

"
    )
    .unwrap();
}

fn make_impl(w: &mut BufWriter<File>, types: &BTreeMap<u32, Type>) {
    write!(
        w,
        "impl Inner {{
    pub fn from_oid(oid: Oid) -> Option<Inner> {{
        match oid {{
",
    )
    .unwrap();

    for (oid, type_) in types {
        writeln!(w, "            {} => Some(Inner::{}),", oid, type_.variant).unwrap();
    }

    writeln!(
        w,
        "            _ => None,
        }}
    }}

    pub fn oid(&self) -> Oid {{
        match *self {{",
    )
    .unwrap();

    for (oid, type_) in types {
        writeln!(w, "            Inner::{} => {},", type_.variant, oid).unwrap();
    }

    writeln!(
        w,
        "            Inner::Other(ref u) => u.oid,
        }}
    }}

    pub fn kind(&self) -> &Kind {{
        match *self {{",
    )
    .unwrap();

    for type_ in types.values() {
        let kind = match &*type_.kind {
            "P" => "Pseudo".to_owned(),
            "A" => format!("Array(Type(Inner::{}))", types[&type_.element].variant),
            "R" => match type_
                .typtype
                .as_ref()
                .expect("range type must have typtype")
                .as_str()
            {
                "r" => format!("Range(Type(Inner::{}))", types[&type_.element].variant),
                "m" => format!("Multirange(Type(Inner::{}))", types[&type_.element].variant),
                typtype => panic!("invalid range typtype {}", typtype),
            },
            _ => "Simple".to_owned(),
        };

        writeln!(
            w,
            "            Inner::{} => {{
                &Kind::{}
            }}",
            type_.variant, kind
        )
        .unwrap();
    }

    writeln!(
        w,
        r#"            Inner::Other(ref u) => &u.kind,
        }}
    }}

    pub fn name(&self) -> &str {{
        match *self {{"#,
    )
    .unwrap();

    for type_ in types.values() {
        writeln!(
            w,
            r#"            Inner::{} => "{}","#,
            type_.variant, type_.name
        )
        .unwrap();
    }

    writeln!(
        w,
        "            Inner::Other(ref u) => &u.name,
        }}
    }}
}}"
    )
    .unwrap();
}

fn make_consts(w: &mut BufWriter<File>, types: &BTreeMap<u32, Type>) {
    write!(w, "impl Type {{").unwrap();
    for type_ in types.values() {
        writeln!(
            w,
            "
    /// {docs}
    pub const {ident}: Type = Type(Inner::{variant});",
            docs = type_.doc,
            ident = type_.ident,
            variant = type_.variant
        )
        .unwrap();
    }

    write!(w, "}}").unwrap();
}
