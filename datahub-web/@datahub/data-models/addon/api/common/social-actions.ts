import { DataModelName } from '@datahub/data-models/constants/entity';
import { getApiRoot, ApiVersion } from '@datahub/utils/api/shared';
import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import {
  ILikesAspect,
  ILikeAction,
  IFollowsAspect,
  IFollowerType
} from '@datahub/metadata-types/types/aspects/social-actions';
import { encodeUrn } from '@datahub/utils/validators/urn';

/**
 * Creates a url for a specific entity by urn and the social action we are constructing
 * @param {DataModelName} entityType - the type of entity for which we are making this request
 * @param {string} urn - identifier for the specific entity for which we want to construct the url
 * @param {string} action - the kind of action we are creating
 */
const getSocialActionsUrl = (entityType: DataModelName, urn: string, action: string): string =>
  `${getApiRoot(ApiVersion.v2)}/${entityType}/${encodeUrn(urn)}/${action}`;

/**
 * Using the socialActionsUrl as a base, create a function that constructs one specifically for
 * like actions
 * @param {DataModelName} entityType - type of entity for which we want to construct this url
 * @param {string} urn - urn identifier for the specific entity for which we are constructing the url
 */
const getLikeActionsUrl = (entityType: DataModelName, urn: string): string =>
  getSocialActionsUrl(entityType, urn, 'likes');

/**
 * Given a Likes aspect from the backend, returns the actual like actions related to that aspect
 * @param actions - the list of like actions that are retrieved from the api
 */
const getLikeActionsFromAspect = ({ actions }: ILikesAspect): Array<ILikeAction> => actions;

/**
 * Given an entity type and urn, construct a getter for which to retrieve likes information
 * @param {DataModelName} entityType - the type of entity for which we want to read like information
 * @param {string} urn - the identifier for the entity for which we want to read like information
 */
export const readLikesForEntity = (entityType: DataModelName, urn: string): Promise<Array<ILikeAction>> =>
  getJSON({ url: getLikeActionsUrl(entityType, urn) }).then(getLikeActionsFromAspect);

/**
 * Given an entity type and urn, post an update request that adds the user to the list of those who
 * like the specified entity
 * @param {DataModelName} entityType - the type of entity for which we want to add a like
 * @param {string} urn - the identifier for the entity to which to add the user's like action
 * @return an updated likes aspect for the entity
 */
export const addLikeForEntity = (entityType: DataModelName, urn: string): Promise<Array<ILikeAction>> =>
  postJSON({ url: `${getLikeActionsUrl(entityType, urn)}/add`, data: {} }).then(getLikeActionsFromAspect);

/**
 * Given an entity type and urn, post an update request that removes the user to the list of those
 * who like the specified entity
 * @param {DataModelName} entityType - the type of entity for which we want to add a like
 * @param {string} urn - the identifier for teh entity to which to add the user's like action
 * @return an updated likes aspect for the entity
 */
export const removeLikeForEntity = (entityType: DataModelName, urn: string): Promise<Array<ILikeAction>> =>
  postJSON({ url: `${getLikeActionsUrl(entityType, urn)}/remove`, data: {} }).then(getLikeActionsFromAspect);

/**
 * Using the socialActionsUrl as a base, create a function that constructs one specifically for
 * follow actions
 * @param {DataModelName} entityType - the type of entity for which we want to construct this urn
 * @param {string} urn - urn identifier for the specific entity instance for which we are
 *  constructing the url
 */
const getFollowActionsUrl = (entityType: DataModelName, urn: string): string =>
  getSocialActionsUrl(entityType, urn, 'follows');

/**
 * Given the followers aspect from the API response, provided as a convenience function we return
 * objects representing the followers themselves
 * @param {Array<IFollowAction>} followers - the list of followers presented as
 *  FollowAction objects
 */
const getFollowersFromAspect = ({ followers }: IFollowsAspect): Array<IFollowerType> =>
  followers.map(({ follower }): IFollowerType => follower);

/**
 * Given an entity type and urn, construct a getter for which to retrieve follows information
 * @param {DataModelName} entityType - the type of entity for which we want to read follow information
 * @param {string} urn - the identifier for the entity for which we want to read follow information
 */
export const readFollowsForEntity = (entityType: DataModelName, urn: string): Promise<Array<IFollowerType>> =>
  getJSON({ url: getFollowActionsUrl(entityType, urn) }).then(getFollowersFromAspect);

/**
 * Given an entity type and urn, construct a getter for which to add the user as a follower
 * @param {DataModelName} entityType - the type of entity for which we want to add the user as a follower
 * @param {string} urn - the identifier for the entity for which we want to update follow information
 * @return an updated follow aspect for the entity, if successful
 */
export const addFollowForEntity = (entityType: DataModelName, urn: string): Promise<Array<IFollowerType>> =>
  postJSON({ url: `${getFollowActionsUrl(entityType, urn)}/add`, data: {} }).then(getFollowersFromAspect);

/**
 * Given an entity type and urn, construct a getter for which to remove the user as a follower
 * @param {DataModelName} entityType - the type of entity for which we want to remove the user as a follower
 * @param {string} urn - the identifier for the entity for which we want to update follow information
 * @return an updated follow aspect for the entity, if successful
 */
export const removeFollowForEntity = (entityType: DataModelName, urn: string): Promise<Array<IFollowerType>> =>
  postJSON({ url: `${getFollowActionsUrl(entityType, urn)}/remove`, data: {} }).then(getFollowersFromAspect);
