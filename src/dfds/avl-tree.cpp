#include "includes/avl-tree.h"
#include <cmath>

bool AVLTree::insert(DataType val) {
    bool inserted = BinarySearchTree::insert(val);

    if(inserted){
        BinarySearchTree::updateNodeBalance(BinarySearchTree::getRootNode());

        Node* unbalancedNode=getRootNode();
        Node* unbalancedParent= nullptr;

        Node* loc=getRootNode();
        Node* locParent= nullptr;
        Node* locChild;

        bool unbalanced=false;

        while (true) {
            if (abs(loc->avlBalance) > 1) {
                unbalancedParent = locParent;
                unbalancedNode = loc;
                unbalanced = true;
            }

            if (loc->val == val) {
                break;
            } else if (loc->val < val) {
                locParent = loc;
                loc = loc->right;
            } else {
                locParent = loc;
                loc = loc->left;
            }
            locChild = loc;
        }

        if(unbalancedNode->avlBalance<0){
            locChild=unbalancedNode->right;
        }
        else{
            locChild=unbalancedNode->left;
        }
        if(!unbalanced){
            return true;
        }

        insertBalance(unbalancedParent, unbalancedNode, locChild, val);
    }
    return inserted;
}

bool AVLTree::remove(DataType val) {
    bool removed = BinarySearchTree::remove(val);

    if(removed){
        if(getRootNode()!= nullptr) {
            BinarySearchTree::updateNodeBalance(BinarySearchTree::getRootNode());
            removeBalance(BinarySearchTree::getRootNode(), nullptr);
        }
    }

    return removed;
}

void AVLTree::insertBalance(Node* unbalancedParent, Node* unbalancedNode, Node* locChild, DataType val) {
    if (val < unbalancedNode->val && val < locChild->val) {
        rightRotate(unbalancedParent, unbalancedNode);
    }
    else if (val > unbalancedNode->val && val > locChild->val) {
        leftRotate(unbalancedParent, unbalancedNode);
    }
    else if (val > unbalancedNode->val && val < locChild->val) {
        rightRotate(unbalancedNode, locChild);
        leftRotate(unbalancedParent, unbalancedNode);
    }
    else if (val < unbalancedNode->val && val > locChild->val) {
        leftRotate(unbalancedNode, locChild);
        rightRotate(unbalancedParent, unbalancedNode);
    }
}

void AVLTree::removeBalance(Node* node, Node* nodeParent) {
    if (node->left != nullptr) {
        removeBalance(node->left, node);
    }

    if (node->right != nullptr) {
        removeBalance(node->right, node);
    }

    BinarySearchTree::updateNodeBalance(BinarySearchTree::getRootNode());

    if (abs(node->avlBalance) > 1) {

        Node* nodeChild = nullptr;

        int leftSubtreeHeight = getDepth(node->left);
        int rightSubtreeHeight = getDepth(node->right);

        if (leftSubtreeHeight > rightSubtreeHeight) {

            nodeChild = node->left;

            if (getDepth(nodeChild->left) >= getDepth(nodeChild->right)) {
                rightRotate(nodeParent, node);
            } else {
                leftRotate(node, nodeChild);
                rightRotate(nodeParent, node);
            }

        } else {

            nodeChild = node->right;

            if (getDepth(nodeChild->left) > getDepth(nodeChild->right)) {
                rightRotate(node, nodeChild);
                leftRotate(nodeParent, node);
            } else {
                leftRotate(nodeParent, node);
            }

        }
    }
}

void AVLTree::leftRotate(Node* parent, Node* node) {
    Node* rightChild = node->right;
    node->right = rightChild->left;
    rightChild->left = node;

    if (parent != nullptr) {
        if (parent->right == node) {
            parent->right = rightChild;
        } else {
            parent->left = rightChild;
        }
    } else {
        *getRootNodeAddress() = rightChild;
    }
}

void AVLTree::rightRotate(Node* parent, Node* node) {
    Node* leftChild = node->left;
    node->left = leftChild->right;
    leftChild->right = node;

    if (parent != nullptr) {
        if (parent->right == node) {
            parent->right = leftChild;
        } else {
            parent->left = leftChild;
        }
    } else {
        *getRootNodeAddress() = leftChild;
    }
}

int AVLTree::getDepth(Node* n) const {
    if (n == nullptr){
        return -1;
    }

    int left_depth = 0, right_depth = 0;
    left_depth  = getDepth(n->left)  + 1;
    right_depth = getDepth(n->right) + 1;

    if (left_depth > right_depth){
        return left_depth;
    }else{
        return right_depth;
    }
}

int AVLTree::getLargerHeight(Node* curr) {
    int leftHeight = 0;
    int rightHeight = 0;

    if(curr->left != nullptr) {
        leftHeight = 1 + getLargerHeight(curr->left);
    }

    if(curr->right != nullptr) {
        rightHeight = 1 + getLargerHeight(curr->right);
    }

    curr->avlBalance = leftHeight - rightHeight;

    if (leftHeight > rightHeight) {
        return leftHeight;
    } else {
        return rightHeight;
    }
}

//Tested