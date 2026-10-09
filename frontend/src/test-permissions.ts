// Mirrors backend auth.Actions so fixtures can grant everything on chosen clusters.
const actions=['read','consume','produce','create','delete','alter-config','reset-offsets','rename','plan','execute','rollback','manage-acls','schema-read','schema-update','connector-read','connector-update','audit','manage-sessions'];
export function allPermissions(...clusters:string[]){return Object.fromEntries(['*',...clusters].map(c=>[c,actions]))}
