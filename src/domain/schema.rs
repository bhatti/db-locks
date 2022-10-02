// @generated automatically by Diesel CLI.

diesel::table! {
    mutexes (mutex_key, tenant_id) {
        mutex_key -> Text,
        tenant_id -> Text,
        version -> Text,
        lease_duration_ms -> BigInt,
        semaphore_key -> Nullable<Text>,
        data -> Nullable<Text>,
        delete_on_release -> Nullable<Bool>,
        locked -> Nullable<Bool>,
        expires_at -> Nullable<Timestamp>,
        created_by -> Nullable<Text>,
        created_at -> Nullable<Timestamp>,
        updated_by -> Nullable<Text>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::table! {
    semaphores (semaphore_key, tenant_id) {
        semaphore_key -> Text,
        tenant_id -> Text,
        version -> Text,
        max_size -> Integer,
        lease_duration_ms -> BigInt,
        data -> Nullable<Text>,
        created_by -> Nullable<Text>,
        created_at -> Nullable<Timestamp>,
        updated_by -> Nullable<Text>,
        updated_at -> Nullable<Timestamp>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    mutexes,
    semaphores,
);
