from pydantic import BaseModel
from typing import List

"""
Including the response models for the different endpoints

Author
------
Valentin Leuthe
Kai-Yan Pan
"""

class Node(BaseModel):
    value: str
    type: str
    image: str

# node object for a relationship 
class RelationshipNode(BaseModel):
    value: str
    collection: str
    type: str

class Community(BaseModel):
    communityId: int
    size: int
    nodes: List[Node]

class Relationship(BaseModel):
    property: str
    type: str
    transaction_event_type: str
    nft_identifier: str
    nft_collection: str
    link_etherscan: str

# relationship object for the community endpoint(s)
class NewRelationship(BaseModel):
    from_: RelationshipNode
    to: RelationshipNode
    relationship: Relationship

class CommunityDetectionResponse(BaseModel):
    communities: List[Community]
    relationships: List[Relationship]

# Node object for the community endpoint(s)
class NewNode(BaseModel):
    value: str
    collection: str
    type: str
    image: str
    link: str
    centrality_score: int

# response model for the endpoint of getting a single community
class NewCommunityResponse(BaseModel):
    community_id: int
    total_node_count: int
    total_nft: int
    nft_share_degods: float
    nft_share_boredapes: float
    nodes: List[NewNode]
    ownership_relations: List[NewRelationship]
    transaction_relations: List[NewRelationship]
    mint_relations: List[NewRelationship]

# community object for the community summary endpoint
class CommunitySummary(BaseModel):
    community_id: int
    size: int
    nft_share_degods: float
    nft_share_boredapes: float

# relationship object for the community summary endpoint
class RelationshipSummary(BaseModel):
    start_node: int
    end_node: int
    count: int

# Response model for the community summmary endpoint
class CommunitySummaryResponse(BaseModel):
    communities: List[CommunitySummary]
    relationships: List[RelationshipSummary]

# Neighbor object for the find NFT/Owner endpoints
class Neighbors(BaseModel):
    value: str
    link: str
    collection: str
    type: str
    image: str

# Response model for the endpoint of finding an Owner
class FindAccountResponse(BaseModel):
    account: str
    link: str
    count_nft_boredapes: int
    count_nft_degods: int
    neighbors: List[Neighbors]
    relationships: List[NewRelationship]

# Response model for the endpoint of finding an NFT
class FindNFTResponse(BaseModel):
    identifier: str
    collection: str
    opensea_url: str
    image_url: str
    neighbors: List[Neighbors]
    relationships: List[NewRelationship]

# ranking object for the ranking response model 
class Ranking(BaseModel):
    identifier: str
    count: int

# Response model for the ranking endpoint
class RankingResponse(BaseModel):
    ranking: List[Ranking]

# Node strucutre for centrality response model 
class CentralityNode(BaseModel):
    value: str
    link: str
    collection: str
    type: str
    image: str
    centrality_score: int

class CentralityRelationship(BaseModel):
    property: List[str]
    link: List[str]
    type: str
    transaction_event_type: List[str]
    nft_identifier: List[str]
    nft_collection: List[str]
    relationship_count: int

class CentralityRelationshipResponse(BaseModel):
    from_: RelationshipNode
    to: RelationshipNode
    relationship: CentralityRelationship

# Response model for the centrality endpoint (centrality on entire network) 
class CentralityResponse(BaseModel):
    nodes: List[CentralityNode]
    ownership_relations: List[CentralityRelationshipResponse]
    transaction_relations: List[CentralityRelationshipResponse]
    mint_relations: List[CentralityRelationshipResponse]

class HistoryResponse(BaseModel):
    dates: List[str]
    counts: List[float]

class HistoryResponseCollectionDistribution(BaseModel):
    collections: List[str]
    counts: List[float]