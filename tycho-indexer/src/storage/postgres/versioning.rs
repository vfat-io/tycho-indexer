//! # Versioning helpers and utilities
//!
//! This module provides access to versioning tools.

use std::{collections::HashMap, hash::Hash};

use chrono::NaiveDateTime;
use diesel::{
    debug_query,
    pg::{sql_types, Pg},
    query_builder::{BoxedSqlQuery, SqlQuery},
    sql_query,
    sql_types::{BigInt, Bytea, Timestamp},
};

use super::orm::{ContractStorage, NewSlot};
use std::fmt::Debug;

pub trait VersionedRow {
    type SortKey: Ord + Clone + Debug;
    type EntityId: Ord + Hash + Debug;
    type Version: Ord + Copy + Debug;

    fn get_id(&self) -> Self::EntityId;

    fn get_sort_key(&self) -> Self::SortKey;

    fn set_valid_to(&mut self, end_version: Self::Version);

    fn get_valid_from(&self) -> Self::Version;
}

pub trait DeltaVersionedRow {
    type Value: Clone + Debug;

    fn get_value(&self) -> Self::Value;
    fn set_previous_value(&mut self, previous_value: Self::Value);
}

pub trait StoredVersionedRow<'a> {
    type EntityId: Ord + Hash + Debug + 'a;
    type PrimaryKey: Into<i64> + Debug;
    type Version: Into<NaiveDateTime> + Copy + Debug;

    fn get_pk(&self) -> Self::PrimaryKey;

    fn get_valid_to(&self) -> Self::Version;

    fn get_entity_id(&'a self) -> Self::EntityId;
}

pub fn set_versioning_attributes<O: VersionedRow>(
    objects: &mut Vec<O>,
) -> HashMap<O::EntityId, O::Version> {
    let mut db_updates = HashMap::new();
    // TODO: potentially assume this
    objects.sort_by_cached_key(|e| e.get_sort_key());

    db_updates.insert(objects[0].get_id(), objects[0].get_valid_from());

    for i in 0..objects.len() - 1 {
        let (head, tail) = objects.split_at_mut(i + 1);
        let current = &mut head[head.len() - 1];
        let next = &tail[0];

        if current.get_id() == next.get_id() {
            current.set_valid_to(next.get_valid_from());
        } else {
            db_updates.insert(next.get_id(), next.get_valid_from());
        }
    }
    db_updates
}

pub fn set_delta_versioning_attributes<O: VersionedRow + DeltaVersionedRow + Debug>(
    objects: &mut Vec<O>,
) -> HashMap<O::EntityId, O::Version> {
    let mut db_updates = HashMap::new();

    objects.sort_by_cached_key(|e| e.get_sort_key());

    dbg!(&objects);

    db_updates.insert(objects[0].get_id(), objects[0].get_valid_from());

    for i in 0..objects.len() - 1 {
        let (head, tail) = objects.split_at_mut(i + 1);
        let current = &mut head[head.len() - 1];
        let next = &mut tail[0];

        dbg!(i);
        dbg!(&current);
        dbg!(&next);

        if current.get_id() == next.get_id() {
            current.set_valid_to(next.get_valid_from());
            next.set_previous_value(current.get_value())
        } else {
            db_updates.insert(next.get_id(), next.get_valid_from());
        }
    }
    db_updates
}

pub fn build_batch_update_query<'a, O: StoredVersionedRow<'a>>(
    objects: &'a [O],
    table_name: &str,
    end_versions: &'a HashMap<O::EntityId, O::Version>,
) -> BoxedSqlQuery<'a, Pg, SqlQuery> {
    dbg!(objects.len());
    let bind_params = (1..=objects.len() * 2)
        .map(|i| if i % 2 == 0 { format!("${}", i) } else { format!("(${}", i) })
        .collect::<Vec<String>>()
        .chunks(2)
        .map(|chunk| chunk.join(", ") + ")")
        .collect::<Vec<String>>()
        .join(", ");
    dbg!(&bind_params);
    let query_str = format!(
        r#"
        UPDATE {} as t set
            valid_to = m.valid_to
        FROM (
            VALUES {}
        ) as m(id, valid_to) 
        WHERE t.id = m.id;
        "#,
        table_name, bind_params
    );
    dbg!(&query_str);
    let mut query = sql_query(query_str).into_boxed();
    for o in objects.iter() {
        let valid_to = *end_versions
            .get(&o.get_entity_id())
            .expect("versions present for all rows");
        query = query
            .bind::<BigInt, _>(o.get_pk().into())
            .bind::<Timestamp, _>(valid_to.into());
    }
    dbg!(debug_query(&query).to_string());
    query
}
