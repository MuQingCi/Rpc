#include "../include/Filter/RpcFilterChain.h"
#include <algorithm>
#include <utility>

void RpcFilterChain::addFilter(std::shared_ptr<RpcFilter> filter)
{
    filters_.push_back(std::move(filter));
}


void RpcFilterChain::removeFilter(std::shared_ptr<RpcFilter> filter)
{
    auto it = std::find(filters_.begin(), filters_.end(), filter);
    if(it != filters_.end())
        filters_.erase(it);
}